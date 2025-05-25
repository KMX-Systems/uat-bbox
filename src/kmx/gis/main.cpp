/// Copyright (c) 2025 - present KMX Systems. All rights reserved.
/// @file main.cpp
#include "kmx/gis/flatgeobuf_processor.hpp"
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

namespace kmx::gis
{
    /// @brief Parses command line arguments to find a user-specified thread count.
    /// Searches for "-t <count>" or "--threads <count>".
    /// @param argc The argument count from main().
    /// @param argv The argument vector from main().
    /// @param default_threads The default number of threads to use if the option is not specified or invalid.
    /// @return The determined number of threads to use, ensuring it is at least 1.
    static std::uint32_t parse_thread_count(const int argc, const char* const argv[], const std::uint32_t default_threads)
    {
        for (int i {1}; i < argc; ++i)
        {
            const std::string arg {argv[i]};
            if ((arg == "-t" || arg == "--threads") && (i + 1) < argc)
                try
                {
                    const int threads_arg {std::stoi(argv[i + 1])};
                    i++;
                    return (threads_arg > 0) ? static_cast<std::uint32_t>(threads_arg) : 1u;
                }
                catch (const std::invalid_argument&)
                {
                    std::cerr << "Warning: Invalid argument for threads: " << argv[i] << ". Using default (" << default_threads << "u)."
                              << std::endl;
                }
                catch (const std::out_of_range&)
                {
                    std::cerr << "Warning: Thread count out of range for " << argv[i] << ". Using default (" << default_threads << "u)."
                              << std::endl;
                }
        }

        return default_threads;
    }

    /// @brief Extracts positional command line arguments for input and output file paths.
    /// This is a basic parser and assumes file paths do not conflict with known options like "-t".
    /// More robust CLI parsing would typically involve a dedicated library.
    /// @param argc The original argument count.
    /// @param argv The original argument vector.
    /// @param[out] input_path Reference to a string where the extracted input file path will be stored.
    /// @param[out] output_path Reference to a string where the extracted output file path will be stored.
    static void extract_positional_args(const int argc, const char* const argv[], std::string& input_path, std::string& output_path)
    {
        for (int i {1}; i < argc; ++i)
        {
            const std::string arg_str {argv[i]};
            if (arg_str == "-t" || arg_str == "--threads")
            {
                i++;
                if (i >= argc)
                    break;
            }
            else if (input_path.empty())
                input_path = arg_str;
            else if (output_path.empty())
                output_path = arg_str;
            else
                std::cerr << "Warning: Unknown or superfluous argument: " << arg_str << std::endl;
        }
    }

    /// @brief Main application logic, encapsulated within the `kmx::gis` namespace.
    /// This function handles command line argument parsing, initializes the `flatgeobuf_processor`,
    /// and executes the feature processing workflow.
    /// @param argc The command line argument count passed from `::main`.
    /// @param argv The command line argument vector passed from `::main`.
    /// @return An integer exit code: 0 for success, non-zero for various error conditions.
    static int run_application(const int argc, const char* const argv[])
    {
        if (argc < 3)
        {
            std::cerr << "Usage: " << (argc > 0 && argv[0] ? argv[0] : "fgb_bbox_extractor")
                      << " <input_polygon.fgb> <output.csv> [-t <num_threads> | --threads <num_threads>]" << std::endl;
            std::cerr << "  <num_threads> is optional. Default is std::thread::hardware_concurrency() - 1 (min 1u)." << std::endl;
            return 1;
        }

        std::string input_fgb_path {};
        std::string output_csv_path {};

        extract_positional_args(argc, argv, input_fgb_path, output_csv_path);

        if (input_fgb_path.empty() || output_csv_path.empty())
        {
            std::cerr << "Error: Input and output file paths must be specified correctly." << std::endl;
            std::cerr << "Usage: " << (argc > 0 && argv[0] ? argv[0] : "fgb_bbox_extractor")
                      << " <input_polygon.fgb> <output.csv> [-t <num_threads> | --threads <num_threads>]" << std::endl;
            return 1;
        }

        const std::uint32_t num_cores {std::thread::hardware_concurrency()};
        std::uint32_t default_num_threads {(num_cores > 1u) ? (num_cores - 1u) : 1u};
        if (num_cores == 0u)
        {
            std::cerr << "Warning: std::thread::hardware_concurrency() returned 0. Defaulting to 1 thread." << std::endl;
            default_num_threads = 1u;
        }

        const std::uint32_t num_threads_to_use {parse_thread_count(argc, argv, default_num_threads)};

        try
        {
            flatgeobuf_processor processor {input_fgb_path, output_csv_path, num_threads_to_use};
            return processor.process_features() ? 0 : 1;
        }
        catch (const std::exception& e)
        {
            std::cerr << "Fatal error: " << e.what() << std::endl;
            return 2;
        }
        catch (...)
        {
            std::cerr << "An unknown fatal error occurred." << std::endl;
            return 3;
        }
    }

} // namespace kmx::gis

/// @brief Global main function, the entry point of the program.
/// This function delegates all application logic to `kmx::gis::run_application`.
/// @param argc The command line argument count.
/// @param argv The command line argument vector.
/// @return The exit code returned by `kmx::gis::run_application`.
int main(const int argc, const char* argv[]) noexcept
{
    try
    {
        return kmx::gis::run_application(argc, argv);
    }
    catch (...)
    {
    }

    return -1;
}
