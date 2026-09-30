require_relative '../../spec_helper'

describe 'Nsq.with_retries' do
  before do
    allow(Nsq).to receive(:sleep)
    allow(Nsq).to receive(:warn)
  end

  [Errno::EPIPE, IOError, EOFError, Errno::ECONNRESET, Nsq::UnexpectedFrameError.new(nil)].each do |error|
    it "retries after #{error.is_a?(Class) ? error : error.class}" do
      calls = 0
      result = Nsq.with_retries(max_attempts: 3) do
        calls += 1
        raise error if calls == 1
        :ok
      end

      expect(result).to eq(:ok)
      expect(calls).to eq(2)
    end
  end

  it 'does not retry an error frame from nsqd' do
    calls = 0
    expect {
      Nsq.with_retries(max_attempts: 3) do
        calls += 1
        raise Nsq::ErrorFrameException, 'E_BAD_TOPIC'
      end
    }.to raise_error(Nsq::ErrorFrameException)
    expect(calls).to eq(1)
  end
end
