module Hive

  class WorkerBee
    attr_accessor :upstream, :poller, :process_method

    def initialize(name: nil, upstream: 'tcp://127.0.0.1:7777')
      @upstream = upstream
      Process.setproctitle(name) if (name);

      reader = RbZMQ::Socket.new(ZMQ::PULL) or abort('socket error')
      reader.connect upstream or abort('connect error')

      @poller = RbZMQ::Poller.new 
      @poller.register(reader, ZMQ::POLLIN)

      Signal.trap('INT') { Process.exit }
      Signal.trap('TERM') { Process.exit }
    end
      
    def buzz( process_method )
      @process_method = process_method
      while true
        @poller.poll(1_000) do |socket|
          msg = socket.recv or abort('recv error')
          self.recv(msg);
        end
      end
    end

    def recv( msg ) 
      @process_method.call(msg)
    end

  end

end

if __FILE__ == $0
  d = WorkerBee.new or abort "Could not create new worker bee"
  d.run
end
