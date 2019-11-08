#ifndef _rate_limiter_h_
#define _rate_limiter_h_

#include <chrono>
#include "rate_limiter_interface.h"

class RateLimiter : public RateLimiterInterface {
public:
    RateLimiter();
    virtual ~RateLimiter() {}


    bool can_request(int permits);

    double get_rate() const;

    void set_rate(double rate);

private:
    void sync(unsigned long long now);

    std::chrono::microseconds claim_next(double permits);

private:
    double interval_;
    double max_permits_;
    double stored_permits_;

    unsigned long long next_free_;

};


#endif