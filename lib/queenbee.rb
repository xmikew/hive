module Hive

  # Mixin that requires a process lamda to be defined. This is passed to the worker bees
  # to perform some action on the messages being send to them

  class QueenBee
    attr_accessor :max_workers, :casts, :default_cast_size, :default_cast_name, :respawn

    def initialize(url: 'tcp://127.0.0.1', port: 7777, max_workers: 10, default_cast_size: 1, default_cast_name: nil, respawn: true)
      @max_workers = max_workers
      @default_cast_size = default_cast_size
      @default_cast_name = default_cast_name if (default_cast_name)
      @respawn = respawn
      @casts ||= {}

      self.install_sig_handlers
    end

    def hatch_cast(name: nil, url: 'tcp://127.0.0.1:7777', process: nil, class_name: nil, size: @default_cast_size)
      if process.nil? && class_name.nil? then
        raise "InvalidUsage - You must specify a class or a process"
      end

      if name && @casts.has_key?(name) then 
        raise "InvalidCastName: Cast '#{name}' already exists!"
      end

      cast_name = name.nil? ? casts.size : name 
      casts[cast_name] =  BeeCast.new(name: name, url: url, size: size, process_method: process, class_name: class_name)

    end

    def adopt_cast( cast ) 
      if cast.name && @casts.key?(cast.name) then
        raise "InvalidCastName: '#{cast.name}' already exists!"
      end
      cast_name = cast.name.nil? ? @casts.length : cast.name
      @casts[cast_name] = cast
    end

    def kill_cast( cast_name=@default_cast_name, signal='TERM' )
      raise "InvalidCastName - no cast name provided" unless(cast_name)
      @casts[cast_name].bee_pids.each { |pid| Process.kill(signal, pid) }
      @casts.delete(cast_name);
    end

    def find_cast_by_pid ( looking_for_pid ) 
      @casts.each { |_,cast| cast.bee_pids.each { |pid| return cast if (looking_for_pid == pid) } }
    end

    def send_work(cast_name=@default_cast_name, msg)
      raise "Cast #{cast_name} does not exist" unless @casts.has_key? cast_name
      casts[cast_name].send_work(msg)
    end

    def send_signal(cast_name=@default_cast_name, signal)
      @casts[cast_name].bee_pids.each { |pid| Process.kill(signal, pid) }
    end

    def install_sig_handlers
      Signal.trap('INT') { hive_collapse 'INT' }
      Signal.trap('TERM') { hive_collapse 'TERM' }
      Signal.trap('CHLD') do 
        while(pid = Process.waitpid(-1, Process::WNOHANG)) do 
          self.find_cast_by_pid(pid).eggs += 1 if (@respawn) 
        end
      end
    end

    def hive_collapse( signal='TERM' )
      @casts.each { |k,v| self.kill_cast(k, signal) }
    end

  end

end
