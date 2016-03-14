#include <cstring>
#include <sstream>
#include <thread>

#include <jni.h>
#include <pthread.h>
#include <sched.h>

#undef  JNIEXPORT
#define JNIEXPORT __attribute__((visibility("default")))

#include "jni/com_indeed_imhotep_service_PinnedThreadFactory.h"

/*
 * Class:     com_indeed_imhotep_service_PinnedThreadFactory_PinnedThread
 * Method:    pin
 * Signature: (I)V
 */
JNIEXPORT void JNICALL
Java_com_indeed_imhotep_service_PinnedThreadFactory_00024PinnedThread_pin
(JNIEnv * env, jobject obj, jint jcpu)
{
    const size_t cpu(jcpu);
    const size_t num_cpus(std::thread::hardware_concurrency());
    if (cpu >= std::min(num_cpus, size_t(CPU_SETSIZE))) {
        std::ostringstream os;
        os <<  __PRETTY_FUNCTION__
           << " bad cpu: " << cpu
           << " num_cpus: " << num_cpus
           << " CPU_SETSIZE: " << CPU_SETSIZE;
        jclass exClass = env->FindClass("java/lang/RuntimeException");
        env->ThrowNew(exClass, os.str().c_str());
    }

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);

    const pthread_t self(pthread_self());
    const int rc(pthread_setaffinity_np(self, sizeof(cpuset), &cpuset));
    if (rc != 0) {
        std::ostringstream os;
        os << __PRETTY_FUNCTION__
           << " could not pin thread cpu " << cpu
           << " err: " << strerror(rc);
        jclass exClass = env->FindClass("java/lang/RuntimeException");
        env->ThrowNew(exClass, os.str().c_str());
    }
}


