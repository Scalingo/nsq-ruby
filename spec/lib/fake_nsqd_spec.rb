require_relative '../spec_helper'

describe FakeNsqd do
  let(:fake) { described_class.new }
  let(:raw_sockets) { [] }

  after do
    raw_sockets.each(&:close)
    fake.stop
  end

  def bounded(seconds = 5, &block)
    FakeNsqd.bounded(seconds, &block)
  end

  def raw_connect(fake)
    socket = TCPSocket.new(fake.host, fake.port)
    raw_sockets << socket
    socket
  end

  def identify(socket, body = '{}')
    socket.write('  V2')
    socket.write(["IDENTIFY\n", body.bytesize, body].pack('a*l>a*'))
  end

  def read_exactly(socket, size, deadline)
    data = ''.b
    while data.bytesize < size
      remaining = deadline - FakeNsqd.monotonic
      return nil if remaining <= 0 || IO.select([socket], nil, nil, remaining).nil?

      chunk = socket.read_nonblock(size - data.bytesize, exception: false)
      return nil if chunk.nil?

      data << chunk unless chunk == :wait_readable
    end
    data
  end

  # => [type, data], or nil on EOF or timeout
  def read_frame(socket, timeout: 2)
    deadline = FakeNsqd.monotonic + timeout
    header = read_exactly(socket, 8, deadline) or return nil
    size, type = header.unpack('l>l>')
    data = read_exactly(socket, size - 4, deadline) or return nil
    [type, data]
  end

  context 'with a real consumer' do
    let!(:consumer) do
      # Resolved outside the bounded thread: RSpec memoizes `let` under a lock this thread holds.
      address = fake.address
      bounded { Nsq::Consumer.new(nsqd: address, topic: 'topic', channel: 'chan', max_in_flight: 1) }
    end

    after { bounded { consumer.terminate } }

    it 'records the handshake and round-trips a message' do
      expect(fake.wait_for_command(session: 1, name: 'MAGIC').args).to eq(['  V2'])
      identify = fake.wait_for_command(session: 1, name: 'IDENTIFY')
      expect(JSON.parse(identify.body)).to include('feature_negotiation' => true)
      expect(fake.wait_for_command(session: 1, name: 'SUB').args).to eq(%w[topic chan])
      expect(fake.wait_for_command(session: 1, name: 'RDY').args).to eq(['1'])

      id = fake.send_message(1, 'hello')
      message = bounded { consumer.pop }
      expect(message.body).to eq('hello')
      expect(message.id).to eq(id)
      expect(message.attempts).to eq(1)

      message.finish
      expect(fake.wait_for_command(session: 1, name: 'FIN').args).to eq([id])
    end

    it 'records the NOP answering a heartbeat' do
      fake.wait_for_command(session: 1, name: 'RDY')
      fake.send_heartbeat(1)
      expect(fake.wait_for_command(session: 1, name: 'NOP')).not_to be_nil
    end

    it 'numbers the session opened by a reconnect' do
      fake.wait_for_command(session: 1, name: 'SUB')
      fake.close_session(1)
      expect(fake.wait_for_command(session: 2, name: 'SUB').args).to eq(%w[topic chan])

      id = fake.send_message(2, 'again')
      expect(bounded { consumer.pop }.id).to eq(id)
    end

    it 'keeps a silenced session open without answering' do
      fake.wait_for_command(session: 1, name: 'RDY')
      fake.session(1).silence!
      fake.send_heartbeat(1)
      fake.wait_for_command(session: 1, name: 'NOP')
      expect(fake.session(1)).not_to be_closed
      expect(fake.events.map(&:session).uniq).to eq([1])
    end
  end

  it 'acknowledges a synchronous producer PUB' do
    producer = bounded { Nsq::Producer.new(nsqd: fake.address, topic: 'topic', synchronous: true) }
    begin
      bounded { producer.write('hello') }
      pub = fake.wait_for_command(session: 1, name: 'PUB')
      expect(pub.args).to eq(['topic'])
      expect(pub.body).to eq('hello')

      bounded { producer.write('a', 'b') }
      expect(fake.wait_for_command(session: 1, name: 'MPUB').body).to eq(%w[a b])
    ensure
      bounded { producer.terminate }
    end
  end

  context 'with reject_before_sub' do
    let(:fake) { described_class.new(reject_before_sub: true) }

    it 'rejects FIN before SUB with E_INVALID and closes' do
      socket = raw_connect(fake)
      identify(socket)
      expect(read_frame(socket)[0]).to eq(FakeNsqd::FRAME_RESPONSE)

      socket.write("FIN 0000000000000001\n")
      expect(read_frame(socket)).to eq([FakeNsqd::FRAME_ERROR, 'E_INVALID cannot FIN in current state'])
      expect(read_frame(socket)).to be_nil
      expect(socket.read_nonblock(1, exception: false)).to be_nil
      expect(fake.session(1)).to be_closed
    end

    it 'accepts RDY after SUB' do
      socket = raw_connect(fake)
      identify(socket)
      read_frame(socket)
      socket.write("SUB topic chan\nRDY 1\n")
      expect(read_frame(socket)).to eq([FakeNsqd::FRAME_RESPONSE, 'OK'])
      fake.wait_for_command(session: 1, name: 'RDY')
      expect(fake.session(1)).not_to be_closed
    end
  end

  it 'holds the IDENTIFY response until released' do
    fake.on_session(1, &:hold_identify!)
    socket = raw_connect(fake)
    identify(socket)
    fake.wait_for_command(session: 1, name: 'IDENTIFY')
    expect(read_frame(socket, timeout: 0.3)).to be_nil

    fake.session(1).release_identify!
    type, data = read_frame(socket)
    expect(type).to eq(FakeNsqd::FRAME_RESPONSE)
    expect(JSON.parse(data)).to include('max_rdy_count' => 2500, 'version' => '1.3.0')
  end

  it 'sends raw bytes such as a partial frame' do
    socket = raw_connect(fake)
    identify(socket)
    read_frame(socket)
    fake.send_raw(1, [104, FakeNsqd::FRAME_MESSAGE, 'x'].pack('l>l>a*'))
    expect(read_frame(socket, timeout: 0.3)).to be_nil
  end

  it 'runs after hooks in the session thread' do
    fake.on_session(1) { |s| s.after('IDENTIFY') { |session, _event| session.close } }
    socket = raw_connect(fake)
    identify(socket)
    expect(read_frame(socket)[0]).to eq(FakeNsqd::FRAME_RESPONSE)
    expect(read_frame(socket)).to be_nil
  end

  it 'stops within a bound, releasing held sessions, and is idempotent' do
    fake.on_session(1, &:hold_identify!)
    socket = raw_connect(fake)
    identify(socket)
    fake.wait_for_command(session: 1, name: 'IDENTIFY')

    bounded(3) { fake.stop }
    bounded(1) { fake.stop }
    expect(read_frame(socket)).to be_nil
    expect { TCPSocket.new(fake.host, fake.port) }.to raise_error(Errno::ECONNREFUSED)
  end

  it 'raises a descriptive error when an event does not arrive' do
    expect { fake.wait_for_command(session: 3, name: 'SUB', timeout: 0.1) }
      .to raise_error(FakeNsqd::Timeout, /SUB on session 3/)
  end
end
