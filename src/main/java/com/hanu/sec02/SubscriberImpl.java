package com.hanu.sec02;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@Slf4j
public class SubscriberImpl<T> implements Subscriber<T> {

    @Getter
    Subscription subscription;

    @Override
    public void onSubscribe(Subscription s) {
        subscription = s;
    }

    @Override
    public void onNext(T s) {
        log.info("Received: {}", s);
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error: {}", t.getMessage(), t);
    }

    @Override
    public void onComplete() {
        log.info("Completed");
    }
}
