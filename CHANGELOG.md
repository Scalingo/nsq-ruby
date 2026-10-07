# Changelog

## Unreleased

### Breaking changes

* `Nsq::NsqdsProducer` always uses synchronous producers: `synchronous` defaults to `true`, and `synchronous: false` raises an `ArgumentError`.
* `Nsq::NsqdsProducer` gives up after trying each nsqd once when `strategy_attempts` is not set, instead of retrying forever. Pass `strategy_attempts: Float::INFINITY` for the previous behaviour.
* `Nsq.with_retries` no longer retries error frames from nsqd (`Nsq::ErrorFrameException`): a synchronous write rejected by nsqd raises immediately.

### Fixes

* A connection that receives an empty frame or an unknown response now stops its loop instead of spinning until it is reconnected.
* A stale stop marker can no longer end the loop of a reconnected connection.
* The connection thread no longer deadlocks when answering a heartbeat or finishing a message over `max_attempts` while the write queue is full.
* TLS connections no longer leave frames unread until the next packet arrives.
* A `PUB`, `MPUB` or `DPUB` whose socket write raised is sent again after the reconnect.
* Synchronous producers match responses to writes in order, no longer keep track of writes in asynchronous mode (memory growth), and fail every pending write when the connection dies.
* `Nsq.with_retries` retries `Errno::EPIPE` and `IOError`.
* `Nsq::UnexpectedFrameError#message` no longer raises a `NameError`.

## 2.5.0 (fork, previously undocumented)

### Breaking changes

* `Nsq::Producer` connects to a single nsqd: the `nsqlookupd` option and a list of nsqds in `nsqd` are no longer supported. Use `Nsq::NsqdsProducer` for several nsqds.

### Features

* `Nsq::Producer` takes `synchronous`, `ok_timeout` and `retry_attempts` to wait for nsqd to acknowledge each write.
* New `Nsq::NsqdsProducer` with `:failover` and `:round_robin` strategies.

## 2.4.1

* Client version connection string should also read from the VERSION file

## 2.4.0

* Remove jeweler gem in favor of using a dynamic gemspec [#66](https://github.com/wistia/nsq-ruby/pull/66)
* Use `URI.encode_www_form_component` instead of `URI.escape` to ensure compatibility with Ruby 3.0+ and fix deprecation warnings in Ruby 2.7. [#65](https://github.com/wistia/nsq-ruby/pull/65)
* Update version of `nsq-cluster` used for tests [#66](https://github.com/wistia/nsq-ruby/pull/66)
* Update TLS certificates used for tests [#66](https://github.com/wistia/nsq-ruby/pull/66)
* GitHub Actions workflow for testing the gem against a matrix of Ruby versions (2.7, 3.0, 3.1, 3.2) introduced [#66](https://github.com/wistia/nsq-ruby/pull/66)
* Support specifying multiple nsqd's when creating a consumer. [#54](https://github.com/wistia/nsq-ruby/pull/54)

## 2.3.1

* Fix `max_attempts` bug (#46)

## 2.3.0

* Add support for `max_attempts` (#43)

## 2.2.0

* Fix memory leak in producer by using a limited `SizedQueue` (#42)

## 2.1.0

* Now compatible with NSQ 1.0 thanks to @pacoguzman (#37).

## 2.0.5

* Bugfix: Do not register `at_exit` handlers for consumers and producers to avoid memory leaks (#34)

## 2.0.4

* Bugfix: Close connection socket when calling `.terminate`.

## 2.0.3

* Bugfix: Ensure write_loop ends; pass message via write_queue (#32)

## 2.0.2

* Bugfix: Use `File.read` instead of `File.open` to avoid leaving open file handles around.

## 2.0.1

* Bugfix: Allow discovery of ephemeral topics when using JRuby.

## 2.0.0

* Enable TLS connections without any client-side keys, certificates or certificate authorities.
* Deprecate `ssl_context` connection setting, rename it to `tls_options` moving forward.
