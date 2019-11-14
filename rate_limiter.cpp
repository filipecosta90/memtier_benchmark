#include <algorithm>
#include <stdexcept>
#include <thread>
#include "rate_limiter.h"

RateLimiter::RateLimiter() : emission_interval(0), ready_to_emit(0), next_allowed_at(0) {

}

bool RateLimiter::can_request(int quantity,struct timeval tv) {

    const unsigned long long now = tv.tv_sec*(uint64_t)1000000+tv.tv_usec;
    if (next_allowed_at > now + emission_interval * quantity)
        return false;
    else {
        claim_next(quantity,now);
        return true;
    }
}

unsigned long long RateLimiter::claim_next(double quantity,unsigned long long now) {

    if (now > next_allowed_at) {
        ready_to_emit = std::max( 0.0, ( (now - next_allowed_at) / emission_interval - quantity ) );
        next_allowed_at = now;
    }
    const unsigned long long wait = next_allowed_at - now;
    const double from_buffered = std::min(quantity, ready_to_emit);
    const double fresh = quantity - from_buffered;
    const long next_free = (long) (fresh * emission_interval);
    next_allowed_at += next_free;
    ready_to_emit -= from_buffered;
    return wait;
}

double RateLimiter::get_rate() const {
    return 1000000.0 / emission_interval;
}

bool RateLimiter::set_rate(long long period_secs,double rate) {
    bool result = false;
    if (rate <= 0.0) {
        result = false;
    } else {
        emission_interval = double ( period_secs * 1000000.0 ) / rate;
    }
    return result;
}
