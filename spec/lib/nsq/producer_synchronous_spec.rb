require_relative '../../spec_helper'

describe Nsq::Producer do
  context 'with a synchronous producer without retry' do
    before do
      @cluster = NsqCluster.new(nsqd_count: 1)
      @nsqd = @cluster.nsqd.first
      @producer = new_producer(@nsqd, synchronous: true, retry_attempts: 0)
    end

    after do
      @producer.terminate if @producer
      @cluster.destroy
    end

    describe '#write' do
      it 'should raise an error when nsqd is down' do
        @nsqd.stop

        expect{
          @producer.write('fail')
        }.to raise_error(Nsq::UnexpectedFrameError)
      end
    end
  end

  context 'with a synchronous producer with retries (default behavior)' do
    before do
      @cluster = NsqCluster.new(nsqd_count: 1)
      @nsqd = @cluster.nsqd.first
      @producer = new_producer(@nsqd, synchronous: true)
    end

    after do
      @producer.terminate if @producer
      @cluster.destroy
    end

    describe '#write' do
      it 'shouldn\'t raise an error when nsqd is down' do
        @nsqd.stop

        Thread.new { sleep 1 ; @nsqd.start }

        expect{ @producer.write('fail') }.not_to raise_error
      end

      it 'raises an error frame from nsqd without retrying' do
        expect {
          Timeout.timeout(0.5) { @producer.write_to_topic('invalid*topic', 'x') }
        }.to raise_error(Nsq::ErrorFrameException, /E_BAD_TOPIC/)
      end
    end
  end

  context 'matching responses to writes' do
    before do
      @cluster = NsqCluster.new(nsqd_count: 1)
      @nsqd = @cluster.nsqd.first
      @producer = new_producer(@nsqd, synchronous: true)

      # writes stop at the connection; responses are injected by hand
      @connection = @producer.instance_variable_get(:@connection)
      @sent = Queue.new
      allow(@connection).to receive(:pub) { @sent << true }

      @write_queue = @producer.instance_variable_get(:@write_queue)
      @response_queue = @producer.instance_variable_get(:@response_queue)
      @first, @second = SizedQueue.new(1), SizedQueue.new(1)
      @write_queue.push(op: :pub, topic: TOPIC, payload: 'first', result: @first)
      @write_queue.push(op: :pub, topic: TOPIC, payload: 'second', result: @second)
      assert_no_timeout { 2.times { @sent.pop } }
    end

    after do
      @producer.terminate if @producer
      @cluster.destroy
    end

    it 'answers writes in the order they were sent' do
      @response_queue.push(Nsq::Response.new(Nsq::Connection::RESPONSE_OK, @connection))
      @response_queue.push(Nsq::Error.new('E_BAD_TOPIC', @connection))

      assert_no_timeout do
        expect(@first.pop).to be_nil
        expect(@second.pop).to be_a(Nsq::ErrorFrameException)
      end
    end

    it 'fails every write in flight when the connection dies' do
      cause = Errno::ECONNRESET.new
      @response_queue.push(cause)

      assert_no_timeout do
        expect(@first.pop).to be(cause)
        expect(@second.pop).to be(cause)
      end
      expect(@producer.instance_variable_get(:@transactions)).to be_empty
    end
  end

  context 'with an asynchronous producer' do
    before do
      @cluster = NsqCluster.new(nsqd_count: 1)
      @nsqd = @cluster.nsqd.first
      @producer = new_producer(@nsqd)
    end

    after do
      @producer.terminate if @producer
      @cluster.destroy
    end

    it 'does not keep track of writes' do
      100.times { |i| @producer.write(i) }
      wait_for { @producer.instance_variable_get(:@write_queue).empty? }
      expect(@producer.instance_variable_get(:@transactions)).to be_empty
    end
  end
end
