package com.looseboxes.ratelimiter;

import com.looseboxes.ratelimiter.rates.Logic;
import com.looseboxes.ratelimiter.rates.Rate;
import com.looseboxes.ratelimiter.util.RateConfig;
import com.looseboxes.ratelimiter.util.RateLimitConfig;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.grid.GridBucketState;
import io.github.bucket4j.grid.ProxyManager;
import io.github.bucket4j.grid.jcache.JCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class RateLimiterImpl<K extends Serializable> implements RateLimiter<K>{

    private static final Logger LOG = LoggerFactory.getLogger(RateLimiterImpl.class);

    private final ProxyManager<K> buckets;
    private final Logic logic;
    private final Rate [] limits;
    private final Supplier<BucketConfiguration>[] configurationSuppliers;
    private final RateExceededListener rateExceededListener;

    public RateLimiterImpl(Cache<K, GridBucketState> cache, RateExceededListener rateExceededListener, RateLimitConfig rateLimitConfig) {
        this.buckets = Bucket4j.extension(JCache.class).proxyManagerForCache(cache);
        this.logic = rateLimitConfig.getLogic();
        this.limits = rateLimitConfig.toRateList().toArray(new Rate[0]);
        this.configurationSuppliers = new Supplier[limits.length];
        List<RateConfig> rateConfigList = rateLimitConfig.getLimits();
        for(int i = 0; i < this.limits.length; i++) {
            RateConfig rateConfig = rateConfigList.get(i);
            BucketConfiguration configuration = getSimpleBucketConfiguration(rateConfig);
            this.configurationSuppliers[i] = () -> configuration;
        }
        this.rateExceededListener = Objects.requireNonNull(rateExceededListener);
    }

    private BucketConfiguration getSimpleBucketConfiguration(RateConfig rateConfig) {
        return Bucket4j.configurationBuilder()
                .addLimit(Bandwidth.simple(rateConfig.getLimit(), getDuration(rateConfig)))
                .build();
    }

    private Duration getDuration(RateConfig rateConfig) {
        return Duration.ofMillis(rateConfig.getTimeUnit().toMillis(rateConfig.getDuration()));
    }

    @Override
    public Rate record(K key) {

        int failCount = 0;

        Rate firstExceededLimit = null;

        for (int i = 0; i < configurationSuppliers.length; i++) {

            Bucket bucket = buckets.getProxy(key, configurationSuppliers[i]);

            if(!bucket.tryConsume(1)) {

                if(firstExceededLimit == null) {
                    firstExceededLimit = limits[i];
                }

                ++failCount;
            }
        }

        // TODO What rate do we use as return value?
        final Rate next = Rate.NONE;

        if(LOG.isDebugEnabled()) {
            LOG.debug("For: {}, limit exceeded: {}, rate: {}, limits: {}",
                    key, firstExceededLimit != null, next, Arrays.toString(limits));
        }

        if((Logic.OR.equals(logic) && failCount > 0) ||
                (Logic.AND.equals(logic) && failCount == configurationSuppliers.length)) {

            rateExceededListener.onRateExceeded(
                    new RateExceededEvent(this, key, next, firstExceededLimit));
        }

        return next;
    }
}
