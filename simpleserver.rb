require 'socket'
require 'thread' #using threads and older Ruby needs this for Mutex

class ParsingError < StandardError
end

class CommandError < StandardError
end

class CommandHandler
	def initialize(datastore)
		@mutex = Mutex.new
		@datastore = datastore
	end

	def dispatch(connection, statement)
			method = ("cmd_" + statement.first.downcase).to_sym
			arguments = statement.slice(1..-1)
			puts "Dispatching \"#{method}\" with arguments (#{arguments.class}):" #DEBUG
			puts arguments #DEBUG
			
			@mutex.synchronize {
				self.send(method, connection, *arguments)
			}
	end
	
	# Handling the case where a command is sent that isn't actually understood
	def method_missing(key, *args)
		raise CommandError, "Command \"#{key}\" is not understood with arguments => #{args}"
	end
	
	private
	
	#DEBUG
	def debug_loc(location, args)
		puts "Got to the #{location} command"
		args.each_with_index do |x, i|
			puts "#{i} => #{x}"
		end
	end
	
	#TODO reply_ messages probably should just return the string and this gets handled at the server level
	def reply_ok(connection)
		connection.puts "+OK\r\n"
	end
	
	def reply_error(connection, message)
		connection.puts "-ERR #{message}\r\n"
	end
	
	def reply_bulk(connection, message)
		if message.nil?
			connection.puts "$-1\r\n"
		else
			connection.puts "$#{message.length}\r\n#{message}\r\n"
		end
	end

	def cmd_(connection, *args)
		#do nothing
		debug_loc("<NULL>", args)
		reply_error(connection, "unknown command ''")
	end
	alias cmd_null cmd_

	def cmd_set(connection, *args)
		debug_loc("SET", args)
		raise CommandError, "SET command did not receive key/value pair #{args}" if args.length < 2	
		#TODO Handle the special case commands
		raise CommandError, "SET command had more than a key/value pair #{args}" if args.length > 2

		@datastore[args[0].to_sym] = args[1]

		reply_ok(connection)
	end
	
	def cmd_get(connection, *args)
		debug_loc("GET", args)
		raise CommandError, "GET command received more than a single key #{args}" if args.length != 1
		
		value = @datastore[args[0].to_sym]		
		reply_bulk(connection, value)
	end
end

class CommandParser
	def parse_length(leading, arg_to_parse)
		return false if arg_to_parse.nil?
		raise ParsingError, "Client argument didn't end in \\r\\n => \"#{arg}\"" if arg_to_parse[-2..-1] != "\r\n"
		arg = arg_to_parse.chomp("\r\n")
		raise ParsingError, "Client argument length did not begin with \"$\": \"#{arg}\"" if not arg.start_with? leading
		raise ParsingError, "Client argument length contained no value \"#{arg}\"" if arg.length < 1
		begin
			Integer(arg[1..-1])
		rescue ArgumentError => e
			raise ParsingError, "Client command count not a valid integer format \"#{arg}\""
		end
	end
	
	def parse(connection)
		begin
			line = connection.gets()
			count = parse_length("*", line)
			return false if not count
			puts "\"#{line.chomp("\r\n")}\" => #{count}" #DEBUG

			statement = []			
			while count > 0
				line = connection.gets()
				length = parse_length("$", line)
				return false if not length
				puts "\"#{line.chomp("\r\n")}\" => #{length}" #DEBUG

				argument = connection.gets()
				return false if argument == nil
				argument.chomp!("\r\n")
				puts "\"#{argument}\" => #{argument}"  #DEBUG

				raise ParsingError, "Argument described length does not match text length #{length} => #{argument}" if argument.length != length

				statement << argument

				count -= 1
			end

			# TODO Should this be an error?  Need to check if the server can take *0 as a statement
			statement << "null" if statement.empty?
			statement
		rescue Errno::ECONNRESET => e
			raise ParsingError, "BASE HANDLER: socket seemed to have died: #{e.to_s}"
		end
	end
end

class Server
	def initialize
		@datastore = Hash.new
		@parser = CommandParser.new
		@handler = CommandHandler.new(@datastore)
	end

	def start
		port = TCPServer.new(6379)
		while true
			connection = port.accept
			Thread.new do
				puts "Client connected to server"
				begin
					while statement = @parser.parse(connection)
						puts "Command parsed successfully"
						@handler.dispatch connection, statement
					end
				rescue Errno::ECONNRESET => e
					puts "Exception on socket: #{e.to_s}"
				rescue ParsingError => e
					puts "Actually got a parsing error raised!!!"
					puts e.to_s
					puts $ERROR_POSITION
				rescue Exception => e
					puts "Something unexpected happend: #{e.class} => #{e.to_s}"
				end
				puts "Server thread terminating"
			end
		end
	end
end

server = Server.new
server.start
