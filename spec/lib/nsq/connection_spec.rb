require_relative '../../spec_helper'

describe Nsq::Connection do
  before do
    @cluster = NsqCluster.new(nsqd_count: 1)
    @nsqd = @cluster.nsqd.first
    @connection = Nsq::Connection.new(host: @cluster.nsqd[0].host, port: @cluster.nsqd[0].tcp_port)
  end
  after do
    @connection.close
    @cluster.destroy
  end


  describe '::new' do
    it 'should raise an exception if it cannot connect to nsqd' do
      @nsqd.stop

      expect{
        Nsq::Connection.new(host: @nsqd.host, port: @nsqd.tcp_port)
      }.to raise_error(Errno::ECONNREFUSED)
    end

    it 'should raise an exception if it connects to something that isn\'t nsqd' do
      expect{
        # try to connect to the HTTP port instead of TCP
        Nsq::Connection.new(host: @nsqd.host, port: @nsqd.http_port)
      }.to raise_error(RuntimeError, /Bad frame type specified/)
    end

    it 'should raise an exception if max_in_flight is above what the server supports' do
      expect{
        # try to connect to the HTTP port instead of TCP
        Nsq::Connection.new(host: @nsqd.host, port: @nsqd.tcp_port, max_in_flight: 1_000_000)
      }.to raise_error(RuntimeError, "max_in_flight is set to 1000000, server only supports 2500")
    end

    %w(tls_options ssl_context).map(&:to_sym).each do |tls_options_key|
      context "when #{tls_options_key} is provided" do
        it 'validates when tls_v1 is true' do
          params = {
            host: @nsqd.host,
            port: @nsqd.tcp_port,
            tls_v1: true
          }
          params[tls_options_key] = {
            certificate: 'blank'
          }

          expect{
            Nsq::Connection.new(params)
          }.to raise_error ArgumentError, /key/
        end
        it 'skips validation when tls_v1 is false' do
          params = {
            host: @nsqd.host,
            port: @nsqd.tcp_port,
            tls_v1: false
          }
          params[tls_options_key] = {
            certificate: 'blank'
          }

          expect{
            Nsq::Connection.new(params)
          }.not_to raise_error
        end
        it 'raises when a key is not provided' do
          params = {
            host: @nsqd.host,
            port: @nsqd.tcp_port,
            tls_v1: true
          }
          params[tls_options_key] = {
            certificate: 'blank'
          }

          expect{
            Nsq::Connection.new(params)
          }.to raise_error ArgumentError, /key/
        end

        it 'raises when a certificate is not provided' do
          params = {
            host: @nsqd.host,
            port: @nsqd.tcp_port,
            tls_v1: true
          }
          params[tls_options_key] = {
            key: 'blank'
          }

          expect{
            Nsq::Connection.new(params)
          }.to raise_error ArgumentError, /certificate/
        end

        it 'raises when the key or cert files are not readable' do
          params = {
            host: @nsqd.host,
            port: @nsqd.tcp_port,
            tls_v1: true
          }
          params[tls_options_key] = {
            key: 'blank',
            certificate: 'blank'
          }

          expect{
            Nsq::Connection.new(params)
          }.to raise_error LoadError, /unreadable/
        end
      end
    end
  end


  describe '#close' do
    it 'can be called multiple times, without issue' do
      expect{
        10.times{@connection.close}
      }.not_to raise_error
    end
  end


  # This is really testing the ability for Connection to reconnect
  describe '#connected?' do
    before do
      # For speedier timeouts
      set_speedy_connection_timeouts!
    end

    it 'should return true when nsqd is up and false when nsqd is down' do
      wait_for{@connection.connected?}
      expect(@connection.connected?).to eq(true)
      @nsqd.stop
      wait_for{!@connection.connected?}
      expect(@connection.connected?).to eq(false)
      @nsqd.start
      wait_for{@connection.connected?}
      expect(@connection.connected?).to eq(true)
    end

  end


  describe 'private methods' do
    describe '#frame_class_for_type' do
      MAX_VALID_TYPE = described_class::FRAME_CLASSES.length - 1
      it "returns a frame class for types 0-#{MAX_VALID_TYPE}" do
        (0..MAX_VALID_TYPE).each do |type|
          expect(
            described_class::FRAME_CLASSES.include?(
              @connection.send(:frame_class_for_type, type)
            )
          ).to be_truthy
        end
      end
      it "raises an error if invalid type > #{MAX_VALID_TYPE} specified" do
        expect {
          @connection.send(:frame_class_for_type, 3)
        }.to raise_error(RuntimeError)
      end
    end


    describe '#handle_response' do
      it 'responds to heartbeat with NOP' do
        frame = Nsq::Response.new(described_class::RESPONSE_HEARTBEAT, @connection)
        expect(@connection).to receive(:nop)
        @connection.send(:handle_response, frame)
      end
    end


    describe '#read_write_loop' do
      before do
        @connection.send(:stop_monitoring_connection)
        @connection.send(:stop_read_write_loop)

        # a socket that IO.select always reports as readable
        readable, writable = IO.pipe
        writable.close
        @nsqd_socket = @connection.instance_variable_get(:@socket)
        @connection.instance_variable_set(:@socket, readable)
      end

      after do
        @connection.instance_variable_get(:@socket).close
        @connection.instance_variable_set(:@socket, @nsqd_socket)
      end

      it 'stops after an empty frame from the socket' do
        allow(@connection).to receive(:receive_frame).and_return(nil)
        expect(@connection).to receive(:die).once.with(
          an_instance_of(Nsq::UnexpectedFrameError).and(having_attributes(message: 'empty frame from socket'))
        )
        assert_no_timeout { @connection.send(:read_write_loop) }
      end

      it 'stops after a response it does not know how to handle' do
        allow(@connection).to receive(:receive_frame).and_return(Nsq::Response.new('bogus', @connection))
        expect(@connection).to receive(:die).once.with(
          an_instance_of(RuntimeError).and(having_attributes(message: /don't know how to handle: bogus/))
        )
        assert_no_timeout { @connection.send(:read_write_loop) }
      end
    end


    describe '#start_read_write_loop' do
      it 'ignores a stop marker left behind by a previous loop' do
        @connection.send(:stop_read_write_loop)
        write_queue = @connection.instance_variable_get(:@write_queue)
        write_queue.push(message: :stop_loop, thread: Thread.new {}.join)

        written = Queue.new
        allow(@connection).to receive(:write_to_socket) { |raw| written << raw }

        @connection.send(:start_read_write_loop)
        @connection.send(:write, "NOP\n")

        assert_no_timeout { expect(written.pop).to eq("NOP\n") }
      end
    end


    describe 'writes issued by the loop thread' do
      before do
        @connection.send(:stop_monitoring_connection)
        @connection.send(:stop_read_write_loop)

        @written = []
        allow(@connection).to receive(:write_to_socket) { |raw| @written << raw }
        allow(@connection).to receive(:die)

        @write_queue = @connection.instance_variable_get(:@write_queue)
        @connection.instance_variable_set(:@write_queue, SelectableQueue.new(1).push(message: 'queued'))

        readable, writable = IO.pipe
        writable.close
        @nsqd_socket = @connection.instance_variable_get(:@socket)
        @connection.instance_variable_set(:@socket, readable)
      end

      after do
        @connection.instance_variable_get(:@socket).close
        @connection.instance_variable_set(:@socket, @nsqd_socket)
        @connection.instance_variable_set(:@write_queue, @write_queue)
      end

      it 'answers a heartbeat when the write queue is full' do
        frame = Nsq::Response.new(described_class::RESPONSE_HEARTBEAT, @connection)
        assert_no_timeout { @connection.send(:handle_response, frame) }
        expect(@written).to eq(["NOP\n"])
      end

      it 'finishes a message over max_attempts when the write queue is full' do
        id = 'a' * 16
        message = Nsq::Message.new([0, 2, id, 'body'].pack('Q>S>a16a*'), @connection)
        allow(@connection).to receive(:receive_frame).and_return(message, nil)
        @connection.instance_variable_set(:@max_attempts, 1)

        assert_no_timeout { @connection.send(:read_write_loop) }
        expect(@written).to include("FIN #{id}\n")
      end
    end


    describe 'write failure in the loop' do
      let(:pub) { ["PUB #{TOPIC}\n", 5, 'hello'].pack('a*l>a*') }
      let(:heartbeat) { Nsq::Response.new(described_class::RESPONSE_HEARTBEAT, @connection) }

      before do
        @connection.send(:stop_monitoring_connection)
        @connection.send(:stop_read_write_loop)
        allow(@connection).to receive(:die)

        @write_queue = @connection.instance_variable_get(:@write_queue)
        @queue = SelectableQueue.new(2)
        @connection.instance_variable_set(:@write_queue, @queue)

        # the first frame is a heartbeat, the next read fails
        reads = 0
        allow(@connection).to receive(:receive_frame) do
          (reads += 1) == 1 ? heartbeat : raise(Errno::ECONNRESET)
        end

        readable, writable = IO.pipe
        writable.close
        @nsqd_socket = @connection.instance_variable_get(:@socket)
        @connection.instance_variable_set(:@socket, readable)
      end

      after do
        @connection.instance_variable_get(:@socket).close
        @connection.instance_variable_set(:@socket, @nsqd_socket)
        @connection.instance_variable_set(:@write_queue, @write_queue)
      end

      def drain(queue)
        items = []
        items << queue.pop(true)[:message] until queue.empty?
        items
      end

      it 'requeues a publish whose write raised' do
        allow(@connection).to receive(:write_to_socket) { |raw| raise Errno::EPIPE if raw == pub }
        @queue.push(message: pub)

        assert_no_timeout { @connection.send(:read_write_loop) }
        expect(drain(@queue)).to eq([pub])
      end

      it 'does not requeue a command that is not a publish' do
        allow(@connection).to receive(:write_to_socket) { |raw| raise Errno::EPIPE if raw.start_with?('FIN') }
        @queue.push(message: "FIN #{'a' * 16}\n")

        assert_no_timeout { @connection.send(:read_write_loop) }
        expect(@queue).to be_empty
      end

      it 'does not requeue a publish that was written before a read failed' do
        allow(@connection).to receive(:write_to_socket)
        @queue.push(message: pub)

        assert_no_timeout { @connection.send(:read_write_loop) }
        expect(@connection).to have_received(:write_to_socket).with(pub).once
        expect(@queue).to be_empty
      end

      it 'drops the publish rather than blocking when the write queue is full' do
        allow(@connection).to receive(:write_to_socket) do |raw|
          next unless raw == pub
          2.times { @queue.push(message: 'filler') }
          raise Errno::EPIPE
        end
        @queue.push(message: pub)

        assert_no_timeout { @connection.send(:read_write_loop) }
        expect(drain(@queue)).to eq(['filler', 'filler'])
      end
    end


    describe '#push_error_pending_writes' do
      before do
        @connection.send(:stop_monitoring_connection)
        @connection.send(:stop_read_write_loop)
        @response_queue = Queue.new
        @connection.instance_variable_set(:@response_queue, @response_queue)
      end

      it 'drains the write queue and reports the cause of death once' do
        3.times { @connection.send(:write, "PUB #{TOPIC}\n") }
        cause = Errno::ECONNRESET.new

        @connection.send(:push_error_pending_writes, cause)

        expect(@connection.instance_variable_get(:@write_queue)).to be_empty
        expect(@response_queue.size).to eq(1)
        expect(@response_queue.pop).to be(cause)
      end

      it 'reports the cause of death even when nothing is queued' do
        @connection.send(:push_error_pending_writes, Errno::ECONNRESET.new)
        expect(@response_queue.size).to eq(1)
      end
    end
  end
end
