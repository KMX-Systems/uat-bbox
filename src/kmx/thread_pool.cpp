/// Copyright (c) 2025 - present KMX Systems. All rights reserved.
/// @file thread_pool.cpp
#include "kmx/thread_pool.hpp"

namespace kmx::gis
{
    void thread_pool::worker_loop() noexcept
    {
        std::function<void()> task_to_execute {};
        while (true)
        {
            {
                std::unique_lock<std::mutex> lock {this->queue_mutex_};
                this->condition_.wait(lock, [this] { return this->stop_ || !this->tasks_.empty(); });

                if (this->stop_ && this->tasks_.empty())
                    return;

                task_to_execute = std::move(this->tasks_.front());
                this->tasks_.pop();
            }
            try
            {
                task_to_execute();
            }
            catch (...)
            {
                // Catch all exceptions from tasks to prevent a worker thread from dying.
                // Optionally log here, but be careful about thread safety of logging mechanism.
                // For now, just swallow to keep thread alive.
            }
        }
    }

    thread_pool::thread_pool(const std::size_t num_threads) noexcept(false)
    {
        workers_.reserve(num_threads);
        for (std::size_t i {}; i < num_threads; ++i)
            workers_.emplace_back(&thread_pool::worker_loop, this);
    }

    thread_pool::~thread_pool() noexcept
    {
        try
        {
            {
                std::unique_lock<std::mutex> lock {queue_mutex_};
                stop_ = true;
            }
            condition_.notify_all();
        }
        catch (...)
        {
            // Swallow all exceptions to ensure destructor is noexcept.
            // Logging here is risky if std::cerr itself can throw under extreme conditions.
        }
    }

} // namespace kmx::gis
