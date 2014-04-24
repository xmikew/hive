require 'rubygems'
require 'eventmachine'
require 'em-http-request'
require 'feedjira'
require 'hive'
require 'pp'

class Drone < Hive::WorkerBee
  def recv (msg)
    puts "..."
  end
end

class CLFeedReader

  include EM::Deferrable
  include Hive

  def initialize(url)
    qb = Hive::QueenBee.new
    qb.hatch_cast(name: 'workers', 
                  url: 'tcp://127.0.0.1:7778', 
                  process: lambda { |msg| puts "processing #{msg}" }, 
                  size: 3
                 );

    qb.hatch_cast(name: 'drones',
                  url: 'tcp://127.0.0.1:7779', 
                  class_name: Drone,
                  size: 1
                 );

    EM.run do
      EM.add_periodic_timer(1) do
        qb.send_work('drones', Random.rand(1000))
        qb.send_work('workers', Random.rand(1000))
      end

    end
  end

    #request = EM::HttpRequest.new(url).get
    #request.callback do
    #  parser = Feedjira::Feed.parse(request.response).entries.each do |entry|
    #    p entry.url
    #  end
    #  self.succeed('\0/')
    #end

    #request.errback do
    #  self.fail("Request failed: #{request.response_header.status}");
    #end
end

if $0 == __FILE__
  url = "http://myrtlebeach.craigslist.org/search/gra?query=tractor&format=rss"
  feed = CLFeedReader.new(url)
end 
