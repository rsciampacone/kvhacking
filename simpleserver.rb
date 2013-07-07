require 'socket'
require 'thread' #using threads and older Ruby needs this for Mutex

module Logger
	def self.init_logging(io)
		# First initializer decides what the IO is for logging output
		@@io ||= io
		@@mutex ||= Mutex.new
	end
	
	def log_info(message)
		log("INFO (#{Time.now.to_s}): #{message}")
		true
	end
	
	def log_error(message)
		log("ERROR (#{Time.now.to_s}): #{message}")
		false
	end
	
	def log_warn(message)
		log("WARN (#{Time.now.to_s}): #{message}")
		true
	end

	private
	
	def log(message)
		@@mutex.synchronize {
			@@io.puts(message)
		}
	end
	
end

class CommandHandler
	include Logger

	def initialize(datastore)
		@mutex = Mutex.new
		@datastore = datastore
	end

	def dispatch(statement, connection)
			method = ("cmd_" + statement.first.downcase).to_sym
			log_info("Client #{connection} dispatching #{method} => #{statement.to_s}")
			
			@mutex.synchronize {
				@connection = connection
				self.send(method, *statement)
				@connection = nil
			}
	end
	
	def log_cmd(command_name, args)
		log_info("Command #{command_name} => #{args.to_s}")
	end

	# Handling the case where a command is sent that isn't actually understood
	alias logger_old_method_missing method_missing
	def method_missing(key, *args)
		if key =~ /^cmd_/ then
			reply_error("unknown command '#{args[0]}'")
			log_warn("Command \"#{key}\" is not understood with arguments => #{args.to_s}")
		else
			logger_old_method_missing(key, *args)
		end
	end
	
	private
	
	#TODO reply_ messages probably should just return the string and this gets handled at the server level
	def reply_ok()
		@connection.puts "+OK\r\n"
	end
	
	def reply_error(message)
		@connection.puts "-ERR #{message}\r\n"
	end
	
	def reply_bulk(message)
		if message.nil?
			@connection.puts "$-1\r\n"
		else
			@connection.puts "$#{message.length}\r\n#{message}\r\n"
		end
	end

	def cmd_(*args)
		reply_error("unknown command ''")
	end
	alias cmd_null cmd_

	def cmd_set(*args)
		#TODO Handle special case or extended commands for "SET"
		return log_error("SET command did not receive key/value pair #{args.to_s}") if args.length < 3
		return log_error("SET command had more than a key/value pair #{args.to_s}") if args.length > 3

		@datastore[args[1].to_sym] = args[2]
		log_cmd("SET", args)
		reply_ok()
	end
	
	def cmd_get(*args)
		log_error("GET command received more than a single key #{args.to_s}") if args.length != 2
		
		value = @datastore[args[1].to_sym]
		log_cmd("GET", args)
		reply_bulk(value)
	end
end

class CommandParser
	include Logger

	def parse_length(leading, arg_to_parse)
		return log_error("Arguments to parse were \"nil\"") if arg_to_parse.nil?
		return log_error("Client argument didn't end in \\r\\n => \"#{arg}\"") if arg_to_parse[-2..-1] != "\r\n"

		arg = arg_to_parse.chomp("\r\n")
		
		return log_error("Client argument length did not begin with \"$\": \"#{arg}\"") if not arg.start_with? leading
		return log_error("Client argument length contained no value \"#{arg}\"") if arg.length < 1

		begin
			Integer(arg[1..-1])
		rescue ArgumentError => e
			log_error("Client command count not a valid integer format \"#{arg}\"")
		end
	end
	
	def parse(connection)
		line = connection.gets()
		count = parse_length("*", line)
		return false if not count
		log_info("Bulk command count \"#{line.chomp("\r\n")}\" => #{count}")

		statement = []
		index = 0	
		while index < count
			line = connection.gets()
			length = parse_length("$", line)
			return false if not length
			log_info("Arg#{index} length \"#{line.chomp("\r\n")}\" => #{length}")

			argument = connection.gets()
			return false if argument == nil
			argument.chomp!("\r\n")
			log_info("Arg#{index} \"#{argument}\"")

			return log_error("Argument described length does not match text length #{length} => #{argument}") if argument.length != length

			statement << argument

			index += 1
		end

		# TODO Should this be an error?  Need to check if the server can take *0 as a statement
		statement << "null" if statement.empty?
		statement
	end
end

class Server
	include Logger
	Logger::init_logging(STDOUT)

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
				log_info("Client #{connection} connected to server")
				begin
					while statement = @parser.parse(connection)
						@handler.dispatch(statement, connection)
					end
				rescue Errno::ECONNRESET => e
					log_error("Exception on socket: #{e.to_s}")
				rescue Exception => e
					log_error("Something unexpected happend: #{e.class} => #{e.to_s}")
				end
				log_info("Client #{connection} terminating")
				connection.close
			end
		end
	end
end

server = Server.new
server.start
