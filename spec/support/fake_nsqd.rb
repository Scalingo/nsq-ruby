require 'json'
require 'socket'

# Scripted fake nsqd for specs. Each accepted TCP connection is a numbered
# Session (1, 2, 3...) served by its own thread.
#
#   fake = FakeNsqd.new(reject_before_sub: false, identify_response: {})
#   fake.address                    # => '127.0.0.1:PORT' (also #host, #port)
#   fake.on_session(2) { |s| s.hold_identify! } # runs before session 2 reads anything
#   fake.wait_for_command(session: 1, name: 'SUB', timeout: 5) # => Event
#   fake.wait_for(timeout: 5) { |event| ... }  # first matching event, past or future
#   fake.events                     # snapshot of every recorded Event
#   fake.session(1, timeout: 5)     # waits until session 1 is accepted
#   fake.send_message(1, 'body', id: nil, attempts: 1) # => message id
#   fake.send_heartbeat(1); fake.send_raw(1, bytes); fake.close_session(1)
#   fake.stop                       # bounded and idempotent; call it in `after`
#
# Events are FakeNsqd::Event(session, name, args, body). Names: 'ACCEPT',
# 'MAGIC', the command names ('IDENTIFY', 'SUB', 'RDY', 'FIN', 'REQ', 'TOUCH',
# 'NOP', 'CLS', 'PUB', 'MPUB', 'DPUB', ...) and 'EOF' when the client closes.
# `body` is the raw IDENTIFY/PUB/DPUB body, or an array of messages for MPUB.
#
# Default replies: IDENTIFY -> JSON response, SUB/PUB/MPUB/DPUB -> OK,
# CLS -> CLOSE_WAIT then close. Everything else is only recorded.
#
# Session controls (callable from the spec thread): send_message,
# send_heartbeat, send_response, send_error, send_raw (e.g. partial frames),
# close, silence! (record but never reply; socket stays open),
# hold_identify! / release_identify!, reject_before_sub!, and
# after(name) { |session, event| ... } to run a hook in the session thread after
# the default reply to `name`.
#
# FakeNsqd.bounded(seconds) { ... } runs a block in a thread and raises
# FakeNsqd::Timeout if it does not finish in time.
class FakeNsqd
  class Timeout < StandardError; end

  Event = Struct.new(:session, :name, :args, :body)

  FRAME_RESPONSE = 0
  FRAME_ERROR = 1
  FRAME_MESSAGE = 2
  STRICT_COMMANDS = %w[FIN RDY REQ TOUCH].freeze
  IGNORED_ERRORS = [IOError, EOFError, SystemCallError].freeze

  def self.bounded(seconds, &block)
    thread = Thread.new(&block)
    thread.report_on_exception = false
    raise Timeout, "block did not finish within #{seconds}s" unless thread.join(seconds)

    thread.value
  ensure
    thread&.kill
  end

  def self.monotonic
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  attr_reader :host, :port, :options

  def initialize(reject_before_sub: false, identify_response: {})
    @options = {
      reject_before_sub: reject_before_sub,
      identify_response: { max_rdy_count: 2500, version: '1.3.0', tls_v1: false }.merge(identify_response)
    }
    @server = TCPServer.new('127.0.0.1', 0)
    @host = '127.0.0.1'
    @port = @server.addr[1]
    @mutex = Mutex.new
    @cond = ConditionVariable.new
    @events = []
    @sessions = {}
    @configurators = {}
    @threads = []
    @stopped = false
    @accept_thread = Thread.new { accept_loop }
    @accept_thread.report_on_exception = false
  end

  def address
    "#{@host}:#{@port}"
  end

  def on_session(number, &block)
    @mutex.synchronize { @configurators[number] = block }
  end

  def events
    @mutex.synchronize { @events.dup }
  end

  def record(session, name, args = [], body = nil)
    event = Event.new(session, name, args, body)
    @mutex.synchronize do
      @events << event
      @cond.broadcast
    end
    event
  end

  def wait_for(timeout: 5, description: 'matching event', &matcher)
    wait_until(timeout, description) { @events.find(&matcher) }
  end

  def wait_for_command(session:, name:, timeout: 5)
    wait_for(timeout: timeout, description: "#{name} on session #{session}") do |e|
      e.session == session && e.name == name
    end
  end

  def session(number, timeout: 5)
    wait_until(timeout, "session #{number} to be accepted") { @sessions[number] }
  end

  def send_message(number, body, **opts)
    session(number).send_message(body, **opts)
  end

  def send_heartbeat(number)
    session(number).send_heartbeat
  end

  def send_raw(number, bytes)
    session(number).send_raw(bytes)
  end

  def close_session(number)
    session(number).close
  end

  def stop(timeout: 2)
    sessions = @mutex.synchronize do
      return if @stopped

      @stopped = true
      @cond.broadcast
      @sessions.values
    end
    @server.close
    sessions.each(&:close)
    deadline = FakeNsqd.monotonic + timeout
    ([@accept_thread] + @threads).each do |thread|
      thread.join([deadline - FakeNsqd.monotonic, 0].max) || thread.kill
    end
    nil
  end

  private

  def wait_until(timeout, description)
    deadline = FakeNsqd.monotonic + timeout
    @mutex.synchronize do
      loop do
        result = yield
        return result if result

        remaining = deadline - FakeNsqd.monotonic
        if remaining <= 0
          seen = @events.map { |e| [e.session, e.name, *e.args].join(' ') }
          raise Timeout, "timed out after #{timeout}s waiting for #{description}; events: #{seen.inspect}"
        end
        @cond.wait(@mutex, remaining)
      end
    end
  end

  def accept_loop
    loop do
      socket = @server.accept
      session = @mutex.synchronize do
        if @stopped
          socket.close
          return
        end
        number = @sessions.size + 1
        @sessions[number] = Session.new(self, number, socket)
      end
      @mutex.synchronize { @configurators[session.number] }&.call(session)
      record(session.number, 'ACCEPT')
      thread = Thread.new { session.run }
      thread.report_on_exception = false
      thread.name = "fake-nsqd-session-#{session.number}"
      @mutex.synchronize { @threads << thread }
    end
  rescue *IGNORED_ERRORS
    nil
  end

  class Session
    attr_reader :number

    def initialize(fake, number, socket)
      @fake = fake
      @number = number
      @socket = socket
      @socket.binmode
      @write_mutex = Mutex.new
      @gate = Queue.new
      @hold_identify = false
      @silent = false
      @subscribed = false
      @closed = false
      @reject_before_sub = fake.options[:reject_before_sub]
      @hooks = Hash.new { |h, k| h[k] = [] }
      @next_id = 0
    end

    def hold_identify!
      @hold_identify = true
    end

    def release_identify!
      @gate << :release
    end

    def silence!
      @silent = true
    end

    def reject_before_sub!
      @reject_before_sub = true
    end

    def after(name, &block)
      @hooks[name] << block
    end

    def subscribed?
      @subscribed
    end

    def closed?
      @closed
    end

    def send_message(body, id: nil, attempts: 1, timestamp: nil)
      id ||= format('%016x', @next_id += 1)
      timestamp ||= (Time.now.to_f * 1_000_000_000).to_i
      write_frame(FRAME_MESSAGE, [timestamp, attempts, id, body].pack('Q>S>a16a*'))
      id
    end

    def send_heartbeat
      send_response('_heartbeat_')
    end

    def send_response(data)
      write_frame(FRAME_RESPONSE, data)
    end

    def send_error(data)
      write_frame(FRAME_ERROR, data)
    end

    def send_raw(bytes)
      @write_mutex.synchronize { @socket.write(bytes) }
    end

    def close
      @closed = true
      @gate << :closed
      @socket.close
    end

    def run
      record('MAGIC', [read_exactly(4)])
      loop do
        line = @socket.gets("\n")
        raise EOFError if line.nil?

        name, *args = line.chomp.split
        handle(name, args)
      end
    rescue EOFError
      record('EOF') unless @closed
    rescue *IGNORED_ERRORS
      nil
    ensure
      close
    end

    private

    def handle(name, args)
      body = read_body(name)
      event = record(name, args, body)
      if @reject_before_sub && !@subscribed && STRICT_COMMANDS.include?(name)
        reply { send_error("E_INVALID cannot #{name} in current state") }
        return close
      end
      case name
      when 'IDENTIFY'
        @gate.pop if @hold_identify
        reply { send_response(JSON.generate(@fake.options[:identify_response])) }
      when 'SUB'
        @subscribed = true
        reply { send_response('OK') }
      when 'PUB', 'MPUB', 'DPUB' then reply { send_response('OK') }
      when 'CLS'
        reply { send_response('CLOSE_WAIT') }
        close
      end
      @hooks[name].each { |hook| hook.call(self, event) }
    end

    def read_body(name)
      return unless %w[IDENTIFY PUB DPUB MPUB].include?(name)

      size = read_int
      return read_exactly(size) unless name == 'MPUB'

      # like nsqd, read MPUB message by message: nsq-ruby's size omits the count
      Array.new(read_int) { read_exactly(read_int) }
    end

    def read_int
      read_exactly(4).unpack1('l>')
    end

    def reply
      yield unless @silent || @closed
    end

    def record(name, args = [], body = nil)
      @fake.record(@number, name, args, body)
    end

    def write_frame(type, data)
      send_raw([data.bytesize + 4, type, data].pack('l>l>a*'))
    end

    def read_exactly(size)
      data = @socket.read(size)
      raise EOFError, 'client closed the connection' if data.nil? || data.bytesize < size

      data
    end
  end
end
