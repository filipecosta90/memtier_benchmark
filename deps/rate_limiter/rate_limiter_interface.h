#ifndef _rate_limiter_interface_h_
#define _rate_limiter_interface_h_

class RateLimiterInterface {
public:
    virtual ~RateLimiterInterface() {}

    virtual long acquire() = 0;

    virtual long acquire(int permits) = 0;

    virtual bool try_acquire(int timeout) = 0;

    virtual bool try_acquire(int permits, int timeout) = 0;

    virtual double get_rate() const = 0;

    virtual void set_rate(double rate) = 0;
};

#endif