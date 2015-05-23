#include "ExecutorService.hpp"
#include <iostream>       // std::cout

void foo()
{
    std::cerr << "foo!\n";
}

void bar(int x)
{
    std::cerr << "bar about to wait " << x << " seconds \n";
    std::this_thread::sleep_for(std::chrono::seconds(x));
    std::cerr << "bar! done sleeping \n";
}

int main()
{

    imhotep::ExecutorService executor_service(2);

    executor_service.enqueue(bar,0);
    executor_service.enqueue(bar,1);
    executor_service.enqueue(foo);
    executor_service.enqueue(bar,0);
    executor_service.enqueue(foo);
    executor_service.enqueue(foo);
    executor_service.enqueue(foo);


  std::cerr << "main, foo and bar now execute concurrently...\n";

  executor_service.awaitCompletion();

  std::cerr << "foo and bar completed.\n";

  return 0;
}
