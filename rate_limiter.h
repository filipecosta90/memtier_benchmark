#ifndef _rate_limiter_h_
#define _rate_limiter_h_

class RateLimiter{
public:
    RateLimiter();
    virtual ~RateLimiter() {}
    bool can_request(int quantity,struct timeval tv);

    double get_rate() const;
    bool set_rate(long long period_secs, double rate);

private:
    unsigned long long claim_next(double quantity,unsigned long long now);

private:
    double emission_interval;
    double ready_to_emit;
    unsigned long long next_allowed_at;
};


#endif