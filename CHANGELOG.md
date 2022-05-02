# Changelog

## To be Released

* Bump nokogiri from 1.11.5 to 1.13.4
* Bump git from 1.3.0 to 1.11.0

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
