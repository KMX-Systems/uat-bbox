/// Copyright (c) 2025 - present KMX Systems. All rights reserved.
/// @file thread_pool.hpp
#pragma once
#ifndef PCH
    #include <condition_variable>
    #include <functional>
    #include <future>
    #include <memory>
    #include <mutex>
    #include <queue>
    #include <stdexcept>
    #include <thread>
    #include <vector>
#endif

namespace kmx::gis
{
    /// @brief A simple thread pool for executing tasks concurrently using `std::jthread`.
    class thread_pool
    {
    public:
        /// @brief Constructs the thread pool and starts worker threads.
        explicit thread_pool(std::size_t num_threads) noexcept(false);
        /// @brief Destructor. Stops and joins worker threads. `noexcept` is ensured.
        ~thread_pool() noexcept;

        /// @brief Enqueues a callable task for execution.
        template <class F, class... Args>
        auto enqueue_task(F&& f, Args&&... args) noexcept(false) -> std::future<typename std::invoke_result_t<F, Args...>>;

        thread_pool(const thread_pool&) = delete;
        thread_pool& operator=(const thread_pool&) = delete;
        thread_pool(thread_pool&&) = delete;
        thread_pool& operator=(thread_pool&&) = delete;

    private:
        /// @brief The main loop executed by each worker thread.
        void worker_loop() noexcept;

        std::vector<std::jthread> workers_ {};
        std::queue<std::function<void()>> tasks_ {};
        std::mutex queue_mutex_ {};
        std::condition_variable condition_ {};
        bool stop_ {};
    };

    template <class F, class... Args>
    auto thread_pool::enqueue_task(F&& f, Args&&... args) noexcept(false) -> std::future<typename std::invoke_result_t<F, Args...>>
    {
        using return_type = typename std::invoke_result_t<F, Args...>;
        auto bound_task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        auto task_ptr = std::make_shared<std::packaged_task<return_type()>>(std::move(bound_task));

        std::future<return_type> future_result {task_ptr->get_future()};
        {
            std::unique_lock<std::mutex> lock {queue_mutex_};
            if (stop_)
                throw std::runtime_error("enqueue_task on stopped thread_pool");
            tasks_.emplace([task_wrapper = task_ptr]() { (*task_wrapper)(); });
        }
        condition_.notify_one();
        return future_result;
    }

} // namespace kmx::gis
