#include <stddef.h>
void calc_radius(const double *emis, double *out, size_t n) {
    for(size_t i=0;i<n;i++){
        double e=emis[i];
        if(e<51126.0) out[i]=10.0;
        else if(e<235483.5677) out[i]=16.67;
        else if(e<1212860.6667) out[i]=23.33;
        else out[i]=40.0;
    }
}
