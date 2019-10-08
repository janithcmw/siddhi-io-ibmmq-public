Siddhi IO IBMMQ
======================================

[![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-ibmmq/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-ibmmq/)
[![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-io-ibmmq.svg)](https://github.com/siddhi-io/siddhi-io-ibmmq/releases)
[![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-io-ibmmq.svg)](https://github.com/siddhi-io/siddhi-io-ibmmq/releases)
[![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-io-ibmmq.svg)](https://github.com/siddhi-io/siddhi-io-ibmmq/issues)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-io-ibmmq.svg)](https://github.com/siddhi-io/siddhi-io-ibmmq/commits/master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-io-ibmmq extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that used to receive and publish events to ibm Message queue.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Downloads
* Versions 3.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.io.ibmmq/siddhi-io-ibmmq/">here</a>.
* Versions 2.x and lower with group id `org.wso2.extension.siddhi.` from  <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.io.ibmmq/siddhi-io-ibmmq">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-ibmmq/api/3.0.0">3.0.0</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-ibmmq/api/3.0.0/#ibmmq-sink">ibmmq</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">Sink</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">IBM MQ sink allows you to publish messages to an IBM MQ broker.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-ibmmq/api/3.0.0/#ibmmq-source">ibmmq</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">Source</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">IBM MQ source allows you to subscribe to an IBM message queue and receive messages. It has the ability to receive messages of the 'map' and 'text' message formats.</p></p></div>

## Dependencies
Please add <a target="_blank" href="http://central.maven.org/maven2/com/ibm/mq/com.ibm.mq.allclient/9.0.5.0/com.ibm.mq.allclient-9.0.5.0.jar">com.ibm.mq.allclient_9.0.5.0_1.0.0.jar</a> and <a target="_blank" href="http://central.maven.org/maven2/javax/jms/javax.jms-api/2.0.1/javax.jms-api-2.0.1.jar">javax.jms-api-2.0.1.jar</a> dependencies

## Installation
For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

# Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.
