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
	
	def reply_bulk(message)
		if message.nil?
			@connection.puts "$-1\r\n"
		else
			@connection.puts "$#{message.length}\r\n#{message}\r\n"
		end
	end

	def reply_integer(value)
		@connection.puts ":#{value.to_s}\r\n"
	end

	# ERROR replies
	def reply_error(message)
		@connection.puts "-ERR #{message}\r\n"
	end

	def reply_key_wrong_type(*args)
		reply_error("Operation against a key holding the wrong kind of value")
	end
	
	def reply_wrong_number_of_arguments(command_name)
		reply_error("wrong number of arguments for '#{command_name}' command")
	end

	def reply_index_out_of_range()
		reply_error("index out of range")
	end
	
	def reply_value_not_integer_or_out_of_range()
		reply_error("value is not an integer or out of range")
	end

	def cmd_(*args)
		reply_error("unknown command ''")
	end
	alias cmd_null cmd_

	#
	# STRING commands
	#

	def cmd_set(*args)
		#TODO Handle special case or extended commands for "SET"
		return reply_wrong_number_of_arguments("set") if args.length != 3

		@datastore[args[1].to_sym] = args[2]
		log_cmd("SET", args)
		reply_ok()
	end
	
	def cmd_get(*args)
		return reply_wrong_number_of_arguments("get") if args.length != 2
		
		value = @datastore[args[1].to_sym]
		log_cmd("GET", args)

		return reply_bulk(nil) if value.nil?
		return reply_key_wrong_type(*args) if not value.is_a? String
		reply_bulk(value)
	end
	
	#
	# LIST commands
	#

	def cmd_lindex(*args)
		return reply_wrong_number_of_arguments("lindex") if args.length != 3
		
		list = @datastore[args[1].to_sym]
		return reply_bulk(nil) if list.nil?
		return reply_key_wrong_type(*args) if not list.is_a? Array

		begin
			index = Integer(args[2], 10)
		rescue ArgumentError => e
			return reply_value_not_integer_or_out_of_range()
		end

		length = list.length
		return reply_index_out_of_range() if (not index < length) or (not (index.abs - 1) < length)

		reply_bulk(list[index])
	end

	def cmd_llen(*args)
		return reply_wrong_number_of_arguments("llen") if args.length != 2
		
		list = @datastore[args[1].to_sym]
		return reply_integer(0) if list.nil?
		return reply_key_wrong_type(*args) if not list.is_a? Array

		reply_integer(list.length)
	end
			
	def cmd_lpop(*args)
		return reply_wrong_number_of_arguments("lpop") if args.length != 2

		list = @datastore[args[1].to_sym]
		return reply_bulk(nil) if list.nil?
		return reply_key_wrong_type(*args) if not list.is_a? Array
		
		value = list.shift
		@datastore[args[1].to_sym] = nil if list.empty?
		log_cmd("LPOP", args)
		reply_bulk(value)
	end

	def cmd_lpush(*args)
		return reply_wrong_number_of_arguments("lpush") if args.length < 3

		list = @datastore[args[1].to_sym]
		list = (@datastore[args[1].to_sym] = []) if list.nil?
		return reply_key_wrong_type(*args) if not list.is_a? Array
		
		args[2..-1].each do | value |
			list.unshift(value)
		end
		log_cmd("LPUSH", args)
		
		reply_integer(args.length - 2)
	end
	
	#
	# HASH commands
	#
	
	def cmd_hset(*args)
		return reply_wrong_number_of_arguments("hset") if args.length != 4

		hash = @datastore[args[1].to_sym]
		hash = (@datastore[args[1].to_sym] = {}) if hash.nil?
		return reply_key_wrong_type(*args) if not hash.is_a? Hash

		log_cmd("HSET", args)
		create_or_update = hash[args[2]].nil? ? 1 : 0
		hash[args[2]] = args[3]
		
		reply_integer(create_or_update)
	end

	def cmd_hget(*args)
		return reply_wrong_number_of_arguments("hget") if args.length != 3

		hash = @datastore[args[1].to_sym]
		return reply_bulk(nil) if hash.nil?
		return reply_key_wrong_type(*args) if not hash.is_a? Hash

		log_cmd("HGET", args)
		reply_bulk(hash[args[2]])
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
			Integer(arg[1..-1], 10)
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
					log_error(e.backtrace)
				end
				log_info("Client #{connection} terminating")
				connection.close
			end
		end
	end
end

server = Server.new
server.start
