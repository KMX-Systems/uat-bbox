/// Copyright (c) 2025 - present KMX Systems. All rights reserved.
/// @file flatgeobuf_processor.cpp
#include "kmx/gis/flatgeobuf_processor.hpp"
#include "flatgeobuf/packedrtree.h"       // For PackedRTree::size, if spatial index is present
#include "kmx/gis/geometry_processor.hpp" // Assuming bounding_box struct is included via types.hpp from here
#include <algorithm>                      // For std::all_of, std::find_if_not
#include <charconv>                       // For std::from_chars, std::to_chars (C++17)
#include <cstring>                        // For std::memcmp
#include <iomanip>                        // For std::setprecision, std::fixed
#include <iostream>                       // For std::cout, std::cerr, std::endl, std::flush
#include <sstream>                        // For std::ostringstream
#include <stdexcept>                      // For std::invalid_argument, std::out_of_range, std::runtime_error
#include <string>                         // For std::stoul, std::to_string, string manipulation
#include <system_error>                   // For std::errc, std::make_error_code

namespace kmx::gis
{
    // Precision for floating-point properties when converted to string.
    static constexpr int property_double_precision {15};

    /// @brief Helper template to read a scalar value from a byte pointer and convert it to a string.
    template <typename T>
    static std::string read_scalar_as_string_static_helper(const std::uint8_t* value_ptr,
                                                           const flatbuffers::uoffset_t remaining_size_at_value,
                                                           flatbuffers::uoffset_t& bytes_read_for_this_scalar) noexcept(false)
    {
        bytes_read_for_this_scalar = {}; // Initialize bytes read
        // Check if there's enough data to read the scalar
        if (sizeof(T) > remaining_size_at_value)
            return {};

        bytes_read_for_this_scalar = sizeof(T);
        return std::to_string(::flatbuffers::ReadScalar<T>(value_ptr));
    }

    /// @brief Specialization of read_scalar_as_string_static_helper for float, with specific precision.
    template <>
    std::string read_scalar_as_string_static_helper<float>(const std::uint8_t* value_ptr,
                                                           const flatbuffers::uoffset_t remaining_size_at_value,
                                                           flatbuffers::uoffset_t& bytes_read_for_this_scalar) noexcept(false)
    {
        bytes_read_for_this_scalar = {};
        if (sizeof(float) > remaining_size_at_value)
            return {};

        bytes_read_for_this_scalar = sizeof(float);
        std::ostringstream oss {}; // std::ostringstream can allocate
        oss << std::fixed << std::setprecision(property_double_precision) << ::flatbuffers::ReadScalar<float>(value_ptr);
        return oss.str();
    }

    /// @brief Specialization of read_scalar_as_string_static_helper for double, with specific precision.
    template <>
    std::string read_scalar_as_string_static_helper<double>(const std::uint8_t* value_ptr,
                                                            const flatbuffers::uoffset_t remaining_size_at_value,
                                                            flatbuffers::uoffset_t& bytes_read_for_this_scalar) noexcept(false)
    {
        bytes_read_for_this_scalar = {};
        if (sizeof(double) > remaining_size_at_value)
            return {};

        bytes_read_for_this_scalar = sizeof(double);
        std::ostringstream oss {}; // std::ostringstream can allocate
        oss << std::fixed << std::setprecision(property_double_precision) << ::flatbuffers::ReadScalar<double>(value_ptr);
        return oss.str();
    }

    /// @brief Converts a FlatBuffers String to a std::string.
    static std::string fbs_string_to_std_string(const ::flatbuffers::String* const fbs_str)
    {
        return (fbs_str != nullptr) ? fbs_str->str() : std::string {};
    }

    // Constructs the processor.
    flatgeobuf_processor::flatgeobuf_processor(const std::string& input_fgb_path, const std::string& output_csv_path,
                                               const std::uint32_t num_threads) noexcept(false):
        input_fgb_path_ {input_fgb_path},
        output_csv_path_ {output_csv_path},
        thread_pool_ {num_threads}
    {
        std::cout << "Thread pool initialized with " << num_threads << " threads.\n";
    }

    // Loads the entire FGB file into an internal buffer.
    bool flatgeobuf_processor::initialize_file_buffer() noexcept(false)
    {
        // Clear any previous state
        fgb_buffer_.clear();
        processing_futures_.clear();
        feature_submission_count_ = {}; // Use default initializer for primitive types
        uat_name_column_index_ = {};    // Reset pre-calculated indices
        uat_code_column_index_ = {};
        county_mn_column_index_ = {}; // Reset county_mn index

        fgb_buffer_ = load_file_to_buffer(input_fgb_path_);

        // Check against minimum FGB file size
        if (fgb_buffer_.empty() || (fgb_buffer_.size() < min_fgb_file_size_))
        {
            std::cerr << "Error: FGB file is too small, empty, or could not be read." << std::endl;
            return false;
        }

        return true;
    }

    // Parses the FGB header from the buffer and validates it.
    bool flatgeobuf_processor::parse_and_validate_header(const FlatGeobuf::Header*& out_fbs_header,
                                                         std::uint32_t& out_header_actual_size) noexcept
    {
        // FlatGeobuf magic bytes
        // Folosim sufixul 'u' pentru a indica unsigned, deși pentru valori mici ca acestea nu e critic.
        static constexpr std::array<std::uint8_t, 8u> expected_magic_bytes {0x66u, 0x67u, 0x62u, 0x03u, 0x66u, 0x67u, 0x62u, 0x00u};

        // Asigură-te că buffer-ul are suficient spațiu pentru magic bytes
        if (fgb_buffer_.size() < expected_magic_bytes.size())
        {
            std::cerr << "Error: File is too small to contain FlatGeobuf magic bytes." << std::endl;
            return false;
        }

        // Compară magic bytes folosind .data() și .size() din std::array
        if (std::memcmp(fgb_buffer_.data(), expected_magic_bytes.data(), expected_magic_bytes.size()) != 0)
        {
            std::cerr << "Error: File is not a valid FlatGeobuf format (magic bytes mismatch)." << std::endl;
            return false;
        }

        std::size_t current_offset {expected_magic_bytes.size()}; // Offset după magic bytes
        // Ensure there's enough data to read the header size
        if ((current_offset + sizeof(std::uint32_t)) > fgb_buffer_.size())
        {
            std::cerr << "Error: File is too small to contain header size." << std::endl;
            return false;
        }

        // Read header size (little-endian uint32_t)
        out_header_actual_size = ::flatbuffers::ReadScalar<std::uint32_t>(fgb_buffer_.data() + current_offset);
        current_offset += sizeof(std::uint32_t);

        // Ensure there's enough data for the full header
        if ((current_offset + out_header_actual_size) > fgb_buffer_.size())
        {
            std::cerr << "Error: File is too small to contain the full header as declared." << std::endl;
            return false;
        }

        // Get a pointer to the header FlatBuffer table
        out_fbs_header = FlatGeobuf::GetHeader(fgb_buffer_.data() + current_offset);
        // Basic check, though GetHeader doesn't usually return null if offset is valid
        if (out_fbs_header == nullptr)
        {
            std::cerr << "Error: Could not parse FlatGeobuf header." << std::endl;
            return false;
        }

        return true;
    }

    // Iterates through features in the FGB buffer, submitting them for processing.
    bool flatgeobuf_processor::submit_feature_tasks(const FlatGeobuf::Header* const fbs_header, std::size_t initial_offset,
                                                    const std::uint32_t coordinate_stride) noexcept(false)
    {
        std::size_t current_offset {initial_offset};
        const std::uint64_t features_to_process {fbs_header->features_count()};

        if (features_to_process > 0u)
            processing_futures_.reserve(features_to_process);

        for (std::uint64_t i {}; i < features_to_process; ++i)
        {
            // Ensure there's enough data to read the feature size
            if ((current_offset + sizeof(std::uint32_t)) > fgb_buffer_.size())
            {
                std::cerr << "Warning: Unexpected end of file while expecting feature " << (i + 1u) << " length." << std::endl;
                return false; // Indicate partial processing
            }

            // Read feature buffer size (little-endian uint32_t)
            const std::uint32_t feature_fbs_buffer_size {::flatbuffers::ReadScalar<std::uint32_t>(fgb_buffer_.data() + current_offset)};

            // Ensure there's enough data for the feature itself
            if ((current_offset + sizeof(std::uint32_t) + feature_fbs_buffer_size) > fgb_buffer_.size())
            {
                std::cerr << "Warning: Unexpected end of file or corrupt feature size for feature " << (i + 1u) << std::endl;
                return false; // Indicate partial processing
            }

            // Get a pointer to the size-prefixed feature FlatBuffer table
            const FlatGeobuf::Feature* const fbs_feature {FlatGeobuf::GetSizePrefixedFeature(fgb_buffer_.data() + current_offset)};
            current_offset += (sizeof(std::uint32_t) + feature_fbs_buffer_size); // Advance offset past this feature

            // Should ideally not happen if sizes are correct
            if (fbs_feature == nullptr)
            {
                std::cerr << "Warning: Could not parse feature " << (i + 1u) << ". Skipping." << std::endl;
                continue;
            }

            // Extract UAT Name using pre-calculated index
            std::string uat_name_val {};
            if (uat_name_column_index_.has_value())
                uat_name_val = get_string_value_for_property(fbs_feature, fbs_header, *uat_name_column_index_);

            // Fallback if property is empty or index was not found
            if (uat_name_val.empty())
            {
                std::ostringstream oss_fallback {};
                oss_fallback << uat_name_fallback_prefix_ << (feature_submission_count_ + 1u);
                uat_name_val = oss_fallback.str();
            }

            // Extract UAT Code using pre-calculated index and parse it
            std::uint32_t uat_code_val {}; // Defaults to 0
            if (uat_code_column_index_.has_value())
            {
                std::string code_value_str = get_string_value_for_property(fbs_feature, fbs_header, *uat_code_column_index_);
                if (!code_value_str.empty())
                {
                    // Trim whitespace from the string before parsing
                    auto trim_start_it = std::find_if_not(code_value_str.begin(), code_value_str.end(), ::isspace);
                    code_value_str.erase(code_value_str.begin(), trim_start_it);
                    auto trim_end_it = std::find_if_not(code_value_str.rbegin(), code_value_str.rend(), ::isspace);
                    code_value_str.erase(trim_end_it.base(), code_value_str.end());

                    // Check again after trim
                    if (!code_value_str.empty())
                    {
                        std::uint32_t parsed_code_val_temp {}; // Use a temporary to avoid altering uat_code_val on parse failure
                        const char* const p_start = code_value_str.data();
                        const char* const p_end = code_value_str.data() + code_value_str.size();
                        auto result = std::from_chars(p_start, p_end, parsed_code_val_temp);

                        // Successful and full parse
                        if ((result.ec == std::errc()) && (result.ptr == p_end))
                            uat_code_val = parsed_code_val_temp; // Assign only on successful parse
                        // Parsing failed or did not consume the entire string
                        else
                            std::cerr << "Warning: UAT code '" << code_value_str << "' for UAT name '" << uat_name_val
                                      << "' could not be fully parsed as uint32_t. Error: " << std::make_error_code(result.ec).message()
                                      << std::endl;
                    }
                }
            }

            // Extract County MN string value using pre-calculated index
            std::string county_mn_str_val {};
            if (county_mn_column_index_.has_value())
                county_mn_str_val = get_string_value_for_property(fbs_feature, fbs_header, *county_mn_column_index_);

            // Construct county_code from the extracted string
            county_code county_mn_val(county_mn_str_val);

            feature_submission_count_++;
            const FlatGeobuf::Geometry* const fbs_geometry_ptr {fbs_feature->geometry()};

            // Prepare data for the processing task
            task_input_data current_task_data {
                std::move(uat_name_val),
                uat_code_val,
                county_mn_val, // Pass the county_code object
                fbs_geometry_ptr,
                coordinate_stride,
                (fbs_geometry_ptr != nullptr) ? fbs_geometry_ptr->type() : FgbGeometryType::Unknown // Use Unknown if no geometry
            };

            // Enqueue the task
            processing_futures_.push_back(
                thread_pool_.enqueue_task(&flatgeobuf_processor::process_single_feature_task, std::move(current_task_data)));

            // Report progress periodically
            if ((feature_submission_count_ % progress_report_interval_) == 0u)
                std::cout << "Submitted " << feature_submission_count_
                          << (features_to_process > 0u ? (" / " + std::to_string(features_to_process)) : "") << " features...\r"
                          << std::flush;
        }

        std::cout << "\nAll " << feature_submission_count_ << " features submitted. Collecting results...\n";
        return true;
    }

    // Collects results from completed feature processing tasks and writes them to CSV.
    bool flatgeobuf_processor::collect_and_write_results(std::ofstream& output_file,
                                                         const std::uint64_t total_features_submitted) noexcept(false)
    {
        std::uint64_t features_written_count {};
        for (auto& fut: processing_futures_)
        {
            try
            {
                task_result result {fut.get()};                                                              // Get result from future
                write_csv_row(output_file, result.uat_name, result.uat_code, result.county_mn, result.bbox); // Pass county_mn
                features_written_count++;

                // Report progress on writing
                if (((features_written_count % progress_report_interval_) == 0u) || (features_written_count == total_features_submitted))
                    std::cout << "Written " << features_written_count << " / " << total_features_submitted << " results to CSV...\r"
                              << std::flush;
            }
            catch (const std::exception& e) // Catch exceptions from fut.get() or write_csv_row()
            {
                std::cerr << "\nError processing or writing a feature result: " << e.what() << std::endl;
                // Continue processing other features despite one error
            }
        }

        std::cout << "\nSuccessfully processed and wrote " << features_written_count << " of " << total_features_submitted << " features."
                  << std::endl;
        // Success if all submitted features were written
        return (features_written_count == total_features_submitted);
    }

    // Main processing function. Executes the workflow of reading, processing, and writing.
    bool flatgeobuf_processor::process_features() noexcept(false)
    {
        // Load FGB file into buffer
        if (!initialize_file_buffer())
            return false;

        const FlatGeobuf::Header* fbs_header {}; // Pointer to the parsed header
        std::uint32_t header_fbs_actual_size {}; // Actual size of the header in bytes
        // Parse and validate header
        if (!parse_and_validate_header(fbs_header, header_fbs_actual_size))
            return false;

        // Pre-calculate UAT name column index
        uat_name_column_index_ = find_property_index_by_name(fbs_header, expected_uat_name_column_);
        if (uat_name_column_index_.has_value())
            std::cout << "Info: Found UAT name property '" << expected_uat_name_column_ << "' at index " << *uat_name_column_index_ << "."
                      << std::endl;
        else
            std::cout << "Warning: Could not find the expected UAT name property '" << expected_uat_name_column_
                      << "'. Fallback names will be used." << std::endl;

        // Pre-calculate UAT code column index
        uat_code_column_index_ = find_property_index_by_name(fbs_header, expected_uat_code_column_);
        if (uat_code_column_index_.has_value())
            std::cout << "Info: Found UAT code property '" << expected_uat_code_column_ << "' at index " << *uat_code_column_index_ << "."
                      << std::endl;
        else
            std::cout << "Warning: Could not find the expected UAT code property '" << expected_uat_code_column_
                      << "'. UAT codes will be missing or 0." << std::endl;

        // Pre-calculate County MN column index
        county_mn_column_index_ = find_property_index_by_name(fbs_header, expected_county_mn_column_);
        if (county_mn_column_index_.has_value())
            std::cout << "Info: Found County MN property '" << expected_county_mn_column_ << "' at index " << *county_mn_column_index_
                      << "." << std::endl;
        else
            std::cout << "Warning: Could not find the expected County MN property '" << expected_county_mn_column_
                      << "'. County MN will be missing." << std::endl;

        // Calculate offset in buffer after the header
        std::size_t current_offset_after_header {
            8u +                    // Magic bytes
            sizeof(std::uint32_t) + // Header size field
            header_fbs_actual_size  // Actual header FlatBuffer size
        };

        std::ofstream output_file {output_csv_path_}; // Open output CSV file
        if (!output_file.is_open())
        {
            std::cerr << "Error: Could not open CSV file for writing: " << output_csv_path_ << std::endl;
            return false;
        }

        print_header_info(fbs_header); // Print info from FGB header

        // Check if the geometry type in the header is supported
        const FgbGeometryType header_geom_type {fbs_header->geometry_type()};
        if ((header_geom_type != FgbGeometryType::Polygon) && (header_geom_type != FgbGeometryType::MultiPolygon))
        {
            std::cerr << "Error: This tool is designed for Polygon/MultiPolygon FGB files. Found: "
                      << FlatGeobuf::EnumNameGeometryType(header_geom_type) << std::endl;
            return false;
        }

        // Determine coordinate stride from header flags
        std::uint32_t coordinate_stride_val {default_coordinate_stride_};
        if (fbs_header->has_z())
            coordinate_stride_val++;
        if (fbs_header->has_m())
            coordinate_stride_val++;

        write_csv_header(output_file); // Write header row to CSV

        // Adjust offset for optional spatial index if present
        if ((fbs_header->index_node_size() > 0u) && (fbs_header->features_count() > 0u))
            current_offset_after_header += FlatGeobuf::PackedRTree::size(fbs_header->features_count(), fbs_header->index_node_size());

        // Submit feature processing tasks to the thread pool
        if (!submit_feature_tasks(fbs_header, current_offset_after_header, coordinate_stride_val))
            // Continue to collect results even if partial submission
            std::cerr << "Feature submission failed or was partial." << std::endl;

        // Collect results and write to CSV
        if (!collect_and_write_results(output_file, feature_submission_count_))
        {
            std::cerr << "Result collection and writing to CSV was not fully successful." << std::endl;
            return false; // Indicate that not all results may have been written
        }

        std::cout << "Output written to: " << output_csv_path_ << std::endl;
        return true;
    }

    // Loads the content of a file into a byte buffer.
    std::vector<std::uint8_t> flatgeobuf_processor::load_file_to_buffer(const std::string& file_path) const noexcept(false)
    {
        // Open file at the end for size
        std::ifstream file_stream {file_path, std::ios::binary | std::ios::ate};
        if (!file_stream)
            throw std::runtime_error("Cannot open file: " + file_path);

        // Get file size
        const std::streamsize size {file_stream.tellg()};
        // Handle empty file
        if (size == 0)
            return {};
        // Handle invalid file size
        if (size < 0)
            throw std::runtime_error("Invalid file size reported for: " + file_path);

        // Seek to beginning to read content
        file_stream.seekg(0, std::ios::beg);
        // Allocate buffer
        std::vector<std::uint8_t> buffer(static_cast<std::size_t>(size));

        // Read file into buffer
        if (!file_stream.read(reinterpret_cast<char*>(buffer.data()), size))
            throw std::runtime_error("Error reading file into buffer: " + file_path);

        return buffer;
    }

    // Finds the index of a property (column) by its name.
    std::optional<std::size_t> flatgeobuf_processor::find_property_index_by_name(
        const FlatGeobuf::Header* const fbs_header, const std::string_view property_name_to_find) const noexcept
    {
        // Check for null header or columns
        if ((fbs_header == nullptr) || (fbs_header->columns() == nullptr))
            return {}; // Return empty optional

        const auto* const fbs_columns {fbs_header->columns()};
        // Iterate through columns to find a match by name
        for (flatbuffers::uoffset_t i {}; i < fbs_columns->size(); ++i)
        {
            const FlatGeobuf::Column* const col {fbs_columns->Get(i)};
            // Check if column and its name exist, then compare the name
            if ((col != nullptr) && (col->name() != nullptr) && (col->name()->string_view() == property_name_to_find))
                return static_cast<std::size_t>(i); // Return index if found
        }

        return {}; // Property not found
    }

    // Reads a property value from the properties blob and converts it to a string.
    std::string flatgeobuf_processor::read_and_convert_property_value_at_offset(
        const FgbColumnType col_type, const std::uint8_t* const properties_data_start, const flatbuffers::uoffset_t value_offset_in_blob,
        const flatbuffers::uoffset_t properties_blob_size, flatbuffers::uoffset_t& bytes_read_for_value) const noexcept(false)
    {
        bytes_read_for_value = {}; // Initialize bytes read
        // Check if offset is out of bounds
        if (value_offset_in_blob >= properties_blob_size)
            return {};

        const std::uint8_t* const current_value_ptr {properties_data_start + value_offset_in_blob};
        const flatbuffers::uoffset_t remaining_size_from_value_ptr {properties_blob_size - value_offset_in_blob};

        // Handle different property types
        switch (col_type)
        {
            case FgbColumnType::Byte:
                return read_scalar_as_string_static_helper<std::int8_t>(current_value_ptr, remaining_size_from_value_ptr,
                                                                        bytes_read_for_value);
            case FgbColumnType::UByte:
                return read_scalar_as_string_static_helper<std::uint8_t>(current_value_ptr, remaining_size_from_value_ptr,
                                                                         bytes_read_for_value);
            case FgbColumnType::Bool:
            {
                std::string bool_val_str {read_scalar_as_string_static_helper<std::uint8_t>(
                    current_value_ptr, remaining_size_from_value_ptr, bytes_read_for_value)};
                // Convert "0" to "false", non-"0" (typically "1") to "true"
                return bool_val_str.empty() ? std::string {} : ((bool_val_str == "0") ? "false" : "true");
            }
            case FgbColumnType::Short:
                return read_scalar_as_string_static_helper<std::int16_t>(current_value_ptr, remaining_size_from_value_ptr,
                                                                         bytes_read_for_value);
            case FgbColumnType::UShort:
                return read_scalar_as_string_static_helper<std::uint16_t>(current_value_ptr, remaining_size_from_value_ptr,
                                                                          bytes_read_for_value);
            case FgbColumnType::Int:
                return read_scalar_as_string_static_helper<std::int32_t>(current_value_ptr, remaining_size_from_value_ptr,
                                                                         bytes_read_for_value);
            case FgbColumnType::UInt:
                return read_scalar_as_string_static_helper<std::uint32_t>(current_value_ptr, remaining_size_from_value_ptr,
                                                                          bytes_read_for_value);
            case FgbColumnType::Long:
                return read_scalar_as_string_static_helper<std::int64_t>(current_value_ptr, remaining_size_from_value_ptr,
                                                                         bytes_read_for_value);
            case FgbColumnType::ULong:
                return read_scalar_as_string_static_helper<std::uint64_t>(current_value_ptr, remaining_size_from_value_ptr,
                                                                          bytes_read_for_value);
            case FgbColumnType::Float:
                return read_scalar_as_string_static_helper<float>(current_value_ptr, remaining_size_from_value_ptr, bytes_read_for_value);
            case FgbColumnType::Double:
                return read_scalar_as_string_static_helper<double>(current_value_ptr, remaining_size_from_value_ptr, bytes_read_for_value);
            case FgbColumnType::String:
            {
                // String is stored as: uint32_t length, followed by char data[length]
                // Check for enough data for string length
                if (sizeof(std::uint32_t) > remaining_size_from_value_ptr)
                    return {};
                // Read string length
                const std::uint32_t len {::flatbuffers::ReadScalar<std::uint32_t>(current_value_ptr)};
                // Check for enough data for string content based on declared length
                if ((sizeof(std::uint32_t) + len) > remaining_size_from_value_ptr)
                {
                    std::cerr << "Warning: String property declared length " << len << " exceeds available data ("
                              << (remaining_size_from_value_ptr - sizeof(std::uint32_t)) << " bytes)." << std::endl;
                    return {};
                }
                bytes_read_for_value = sizeof(std::uint32_t) + len;
                return std::string(reinterpret_cast<const char*>(current_value_ptr + sizeof(std::uint32_t)), len);
            }
            // Other types like DateTime, Json, Binary are not handled here explicitly
            default:
                std::cerr << "Warning: Unhandled property type encountered in read_and_convert: " << static_cast<int>(col_type)
                          << std::endl;
                return {};
        }
    }

    // Calculates the number of bytes a property value occupies in the properties blob, to skip it.
    flatbuffers::uoffset_t flatgeobuf_processor::skip_property_value_at_offset(
        const FgbColumnType col_type, const std::uint8_t* const properties_data_start, const flatbuffers::uoffset_t value_offset_in_blob,
        const flatbuffers::uoffset_t properties_blob_size) const noexcept
    {
        // Nothing to skip if offset is out of bounds
        if (value_offset_in_blob >= properties_blob_size)
            return {};

        const std::uint8_t* const value_ptr {properties_data_start + value_offset_in_blob};
        const flatbuffers::uoffset_t remaining_size {properties_blob_size - value_offset_in_blob};
        flatbuffers::uoffset_t bytes_to_skip {};

        // Helper lambda to check if we can skip 'needed' bytes
        auto can_skip = [&](std::size_t needed) { return (needed <= remaining_size); };

        switch (col_type)
        {
            case FgbColumnType::Byte:
                bytes_to_skip = sizeof(std::int8_t);
                break;
            case FgbColumnType::UByte:
                bytes_to_skip = sizeof(std::uint8_t);
                break;
            case FgbColumnType::Bool:
                bytes_to_skip = sizeof(std::uint8_t);
                break; // Bool is stored as byte
            case FgbColumnType::Short:
                bytes_to_skip = sizeof(std::int16_t);
                break;
            case FgbColumnType::UShort:
                bytes_to_skip = sizeof(std::uint16_t);
                break;
            case FgbColumnType::Int:
                bytes_to_skip = sizeof(std::int32_t);
                break;
            case FgbColumnType::UInt:
                bytes_to_skip = sizeof(std::uint32_t);
                break;
            case FgbColumnType::Long:
                bytes_to_skip = sizeof(std::int64_t);
                break;
            case FgbColumnType::ULong:
                bytes_to_skip = sizeof(std::uint64_t);
                break;
            case FgbColumnType::Float:
                bytes_to_skip = sizeof(float);
                break;
            case FgbColumnType::Double:
                bytes_to_skip = sizeof(double);
                break;
            case FgbColumnType::String:
            {
                // Cannot even read length
                if (!can_skip(sizeof(std::uint32_t)))
                    return remaining_size; // Skip all remaining as a precaution
                const std::uint32_t len {::flatbuffers::ReadScalar<std::uint32_t>(value_ptr)};
                bytes_to_skip = sizeof(std::uint32_t) + len;
                break;
            }
            // For Json, DateTime, Binary, their size might be more complex or like String (length-prefixed).
            // Assuming for now they are length-prefixed like String for skipping purposes.
            case FgbColumnType::Json:
            case FgbColumnType::DateTime:
            case FgbColumnType::Binary:
            default: // Includes Unknown and any other unhandled types
                std::cerr << "Warning: Attempting to skip unhandled or complex property type " << static_cast<int>(col_type)
                          << " in skip_property_value_at_offset. Assuming length-prefixed like String/Binary." << std::endl;
                // Attempt to read length as if it were a string/binary, as this is a common pattern for variable-size types.
                if (!can_skip(sizeof(std::uint32_t)))
                {
                    std::cerr << "  Cannot even read length for presumed variable-size type. Skipping all remaining." << std::endl;
                    return remaining_size;
                }
                const std::uint32_t len {::flatbuffers::ReadScalar<std::uint32_t>(value_ptr)};
                bytes_to_skip = sizeof(std::uint32_t) + len;
                if (!can_skip(bytes_to_skip))
                {
                    std::cerr << "  Calculated skip size " << bytes_to_skip << " for unhandled type " << static_cast<int>(col_type)
                              << " exceeds remaining data (" << remaining_size << "). Skipping all remaining." << std::endl;
                    return remaining_size;
                }
                break;
        }

        // Ensure we don't skip more than available
        return can_skip(bytes_to_skip) ? bytes_to_skip : remaining_size;
    }

    // Retrieves the string value of a specific property for a feature by its column index.
    std::string flatgeobuf_processor::get_string_value_for_property(const FlatGeobuf::Feature* const fbs_feature,
                                                                    const FlatGeobuf::Header* const fbs_header,
                                                                    const std::size_t target_column_index) const noexcept(false)
    {
        // Validate input parameters
        if ((fbs_feature == nullptr) || (fbs_feature->properties() == nullptr) || (fbs_header == nullptr) ||
            (fbs_header->columns() == nullptr) || (target_column_index >= fbs_header->columns()->size()))
            return {};

        const auto* const properties_fbs_vector {fbs_feature->properties()};
        const std::uint8_t* const properties_data_ptr {properties_fbs_vector->data()};
        const flatbuffers::uoffset_t properties_blob_size {properties_fbs_vector->size()};
        flatbuffers::uoffset_t current_offset_in_blob {};

        // Properties are stored as: (uint16_t column_index, T value)...
        while (current_offset_in_blob < properties_blob_size)
        {
            // Check if we can read the column index (uint16_t)
            if ((current_offset_in_blob + sizeof(std::uint16_t)) > properties_blob_size)
                break; // Not enough data for another column index

            const std::uint16_t current_fbs_col_idx {
                ::flatbuffers::ReadScalar<std::uint16_t>(properties_data_ptr + current_offset_in_blob)};
            current_offset_in_blob += sizeof(std::uint16_t);

            // Validate column index from properties data
            if (current_fbs_col_idx >= fbs_header->columns()->size())
            {
                std::cerr << "Warning: Corrupt property column index " << current_fbs_col_idx
                          << " encountered for feature. Offset: " << (current_offset_in_blob - sizeof(std::uint16_t)) << std::endl;
                return {};
            }

            const FlatGeobuf::Column* const column_schema {fbs_header->columns()->Get(current_fbs_col_idx)};
            // Should not happen if index is valid
            if (column_schema == nullptr)
            {
                std::cerr << "Warning: Could not get column schema for index " << current_fbs_col_idx << "." << std::endl;
                return {};
            }

            const FgbColumnType col_type {column_schema->type()};

            // If this is the target column, read its value
            if (static_cast<std::size_t>(current_fbs_col_idx) == target_column_index)
            {
                flatbuffers::uoffset_t bytes_read_for_this_value {};
                return read_and_convert_property_value_at_offset(col_type, properties_data_ptr, current_offset_in_blob,
                                                                 properties_blob_size, bytes_read_for_this_value);
            }
            // Otherwise, skip this property's value to get to the next one
            else
            {
                const flatbuffers::uoffset_t bytes_to_skip_for_value =
                    skip_property_value_at_offset(col_type, properties_data_ptr, current_offset_in_blob, properties_blob_size);

                // Ensure skipping does not go past the end of the blob
                if (bytes_to_skip_for_value > (properties_blob_size - current_offset_in_blob))
                {
                    std::cerr << "Warning: Property skipping would read past blob end for column index " << current_fbs_col_idx << " (type "
                              << static_cast<int>(col_type) << "). Offset: " << current_offset_in_blob << std::endl;
                    break;
                }

                current_offset_in_blob += bytes_to_skip_for_value;
            }
        }

        return {}; // Target column not found
    }

    // Prints basic information from the FGB header to standard output.
    void flatgeobuf_processor::print_header_info(const FlatGeobuf::Header* const fbs_header) const noexcept
    {
        // Guard against null header
        if (fbs_header == nullptr)
            return;
        std::cout << "Processing FGB file: " << fbs_string_to_std_string(fbs_header->name()) << std::endl;
        const FgbGeometryType header_geom_type {fbs_header->geometry_type()};
        std::cout << "Header Geometry Type: " << FlatGeobuf::EnumNameGeometryType(header_geom_type) << std::endl;
        std::cout << "Feature count (from header): " << fbs_header->features_count() << std::endl;
        if (fbs_header->has_z())
            std::cout << "Data includes Z coordinates." << std::endl;
        if (fbs_header->has_m())
            std::cout << "Data includes M coordinates." << std::endl;
    }

    // Writes the CSV header row to the output file stream.
    void flatgeobuf_processor::write_csv_header(std::ofstream& out_file) const noexcept(false)
    {
        out_file << "uat_name" << csv_delimiter_ << "uat_code" << csv_delimiter_ << "county_code_mn" << csv_delimiter_ << "min_x"
                 << csv_delimiter_ << "min_y" << csv_delimiter_ << "max_x" << csv_delimiter_ << "max_y" << csv_delimiter_ << "bbox_area_km2"
                 << csv_newline_;
    }

    /// @brief Helper function to write a string to a CSV stream, escaping it if necessary.
    static void write_csv_escaped_string(std::ofstream& out_file, const std::string& value) noexcept(false)
    {
        // These constants are specific to CSV escaping logic.
        static constexpr char local_csv_quote_char {'"'};
        static constexpr std::string_view local_csv_escaped_quote_str {"\"\""};

        // Check if the string contains characters that need escaping (comma, double quote, newline)
        if (value.find_first_of(",\"\n") != std::string::npos)
        {
            out_file << local_csv_quote_char; // Enclose in double quotes
            // Iterate through characters to escape quotes
            for (const char c: value)
                if (c == local_csv_quote_char)
                    out_file << local_csv_escaped_quote_str; // Escape double quotes
                else
                    out_file << c;
            out_file << local_csv_quote_char;
        }
        // No escaping needed
        else
            out_file << value;
    }

    // Writes a single CSV data row for a feature to the output file stream.
    void flatgeobuf_processor::write_csv_row(std::ofstream& out_file, const std::string& uat_name, std::uint32_t uat_code,
                                             const county_code& county_mn, const bounding_box& bbox) const noexcept(false)
    {
        // Write UAT name (escaped)
        write_csv_escaped_string(out_file, uat_name);
        // Add delimiter
        out_file << csv_delimiter_;

        // Write UAT code directly
        out_file << uat_code;

        // Add delimiter
        out_file << csv_delimiter_;
        // Write County MN code (escaped, using its to_string method)
        write_csv_escaped_string(out_file, county_mn.to_string());
        // Add delimiter
        out_file << csv_delimiter_;

        // Store and restore original stream flags and precision for coordinate formatting
        const std::ios_base::fmtflags original_flags_coords {out_file.flags()};
        const std::streamsize original_precision_coords {out_file.precision()};
        out_file << std::fixed << std::setprecision(csv_coordinate_precision);

        if (bbox.is_valid)
            out_file << bbox.min_x << csv_delimiter_ << bbox.min_y << csv_delimiter_ << bbox.max_x << csv_delimiter_ << bbox.max_y;
        // Write empty fields if bbox is not valid
        else
            out_file << csv_delimiter_ << csv_delimiter_ << csv_delimiter_;

        // Restore original stream flags and precision after writing coordinates
        out_file.flags(original_flags_coords);
        out_file.precision(original_precision_coords);

        // Add delimiter
        out_file << csv_delimiter_;

        // Calculate and write bounding box area if valid
        if (bbox.is_valid)
        {
            const double width_m {(bbox.max_x - bbox.min_x)};
            const double height_m {(bbox.max_y - bbox.min_y)};
            const double area_sq_m {(width_m * height_m)};
            const double area_sq_km {(area_sq_m / square_meters_in_square_kilometer_)};

            // Store and restore original stream flags and precision for area formatting
            const std::ios_base::fmtflags original_flags_area {out_file.flags()};
            const std::streamsize original_precision_area {out_file.precision()};

            // Write area with fixed precision (1 decimal place)
            out_file << std::fixed << std::setprecision(1) << area_sq_km;

            // Restore original stream flags and precision
            out_file.flags(original_flags_area);
            out_file.precision(original_precision_area);
        }
        // End the CSV row
        out_file << csv_newline_;
    }

    // Static method to process a single feature; designed to be run in a separate thread.
    task_result flatgeobuf_processor::process_single_feature_task(task_input_data task_data) noexcept
    {
        // Calculate bounding box for the feature's geometry
        bounding_box bbox {geometry_processor::calculate_for_geometry(task_data.geometry_ptr, task_data.coordinate_stride,
                                                                      task_data.actual_geometry_type)};
        // Construct and return the result, moving data where possible
        return {std::move(task_data.uat_name), task_data.uat_code, task_data.county_mn, bbox};
    }

} // namespace kmx::gis
