module Hive

  class BeeCast

    attr_accessor :name, :bee_pids, :size, :writer, :eggs
    attr_reader :class_name, :process_method, :upstream

    def initialize(name: nil, url: nil, process_method: nil, class_name: nil, size: 1, eggs: 0)
      @eggs = eggs
      @size = size
      @name = name
      @class_name = class_name
      @process_method = process_method
      @upstream = url
      @bee_pids = Array.new

      @writer = RbZMQ::Socket.new(ZMQ::PUSH) or abort 'Socket error'
      @writer.bind(url) or abort 'bind error'

      @size.times do
        hatch 
      end
    end

    def hatch( class_name: @class_name, process_method: @process_method, upstream: @upstream)
        pid = fork
        if !pid
          if (class_name) 
            worker = class_name.new(:name => "#{@name} [#{@bee_pids.length}]", :upstream => upstream)
          else
            worker = WorkerBee.new(:name => "#{@name} [#{@bee_pids.length}]", :upstream => upstream)
          end
          worker.buzz(process_method)
        end
        @bee_pids.push(pid)
    end

    def send_work (data)
      @eggs.times { hatch; @eggs -= 1 }
      @writer.send(data, ZMQ::NOBLOCK)
    end

  end

end
