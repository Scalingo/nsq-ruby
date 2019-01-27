module Nsq
  # Raised when nsqlookupd discovery fails
  class DiscoveryException < Exception; end

  class ErrorFrameException < Exception; end

  class UnexpectedFrameError < Exception
    def initialize(frame)
      @frame = frame
    end

    def message
      if @frame
        return "unexpected frame value #{frame}"
      end
      return 'empty frame from socket'
    end
  end
end
