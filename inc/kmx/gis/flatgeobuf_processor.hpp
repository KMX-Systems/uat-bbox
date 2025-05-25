/// Copyright (c) 2025 - present KMX Systems. All rights reserved.
/// @file flatgeobuf_processor.hpp
#pragma once
#ifndef PCH
    #include "flatgeobuf/feature_generated.h"
    #include "kmx/gis/types.hpp"
    #include "kmx/thread_pool.hpp"
    #include <fstream>
    #include <future>
    #include <optional>
    #include <string>
    #include <string_view>
    #include <vector>
#endif

namespace kmx::gis
{
    // Type aliases for FlatGeobuf enums
    using FgbColumnType = FlatGeobuf::ColumnType;
    using FgbGeometryType = FlatGeobuf::GeometryType;

    /// @brief Orchestrates reading FlatGeobuf files, processing features for bounding boxes,
    /// and writing results to a CSV file.
    /// This class handles low-level interaction with FlatBuffer-generated structures.
    class flatgeobuf_processor
    {
    public:
        /// @brief Constructs the processor.
        /// @param input_fgb_path Path to the input FlatGeobuf file (must be Polygon or MultiPolygon).
        /// @param output_csv_path Path for the output CSV file.
        /// @param num_threads Number of worker threads for parallel processing.
        flatgeobuf_processor(const std::string& input_fgb_path, const std::string& output_csv_path,
                             std::uint32_t num_threads) noexcept(false);

        /// @brief Main processing function. Executes the workflow of reading, processing, and writing.
        /// @return True on success, false on controlled failure (e.g., file format error).
        bool process_features() noexcept(false);

    private:
        // File and Header Processing
        /// @brief Loads the entire FGB file into an internal buffer.
        /// @return True if successful, false if file cannot be read or is empty.
        bool initialize_file_buffer() noexcept(false);

        /// @brief Parses the FGB header from the buffer and validates it.
        /// @param[out] out_fbs_header Pointer to store the parsed FlatBuffer Header.
        /// @param[out] out_header_actual_size Size of the header read from the file.
        /// @return True if header is valid and parsed, false otherwise.
        bool parse_and_validate_header(const FlatGeobuf::Header*& out_fbs_header, std::uint32_t& out_header_actual_size) noexcept;

        // Feature Processing Orchestration
        /// @brief Iterates through features in the FGB buffer, submitting them for processing.
        /// @param fbs_header The parsed FGB header.
        /// @param initial_offset Offset in the buffer after the header (and optional index).
        /// @param coordinate_stride Calculated coordinate stride for geometries.
        /// @return True if all features were submitted successfully, false on error.
        bool submit_feature_tasks(const FlatGeobuf::Header* fbs_header, std::size_t initial_offset,
                                  std::uint32_t coordinate_stride) noexcept(false);

        /// @brief Collects results from completed feature processing tasks and writes them to CSV.
        /// @param output_file The output CSV file stream.
        /// @param total_features_submitted The total number of features that were submitted for processing.
        /// @return True if all results collected and written, false otherwise.
        bool collect_and_write_results(std::ofstream& output_file, std::uint64_t total_features_submitted) noexcept(false);

        // Property Parsing Helpers
        /// @brief Reads a property value from the properties blob at a given offset and converts it to a string.
        /// @param col_type The FlatGeobuf column type of the property.
        /// @param properties_data_start Pointer to the start of the properties data blob for the feature.
        /// @param value_offset_in_blob Offset within the properties blob where the value starts.
        /// @param properties_blob_size Total size of the properties blob.
        /// @param[out] bytes_read_for_value Number of bytes read from the blob for this specific value.
        /// @return The property value as a string. Returns an empty string on error or if conversion is not possible.
        std::string read_and_convert_property_value_at_offset(FgbColumnType col_type, const uint8_t* properties_data_start,
                                                              flatbuffers::uoffset_t value_offset_in_blob,
                                                              flatbuffers::uoffset_t properties_blob_size,
                                                              flatbuffers::uoffset_t& bytes_read_for_value) const noexcept(false);

        /// @brief Calculates the number of bytes a property value occupies in the properties blob, to skip it.
        /// @param col_type The FlatGeobuf column type of the property.
        /// @param properties_data_start Pointer to the start of the properties data blob for the feature.
        /// @param value_offset_in_blob Offset within the properties blob where the value starts.
        /// @param properties_blob_size Total size of the properties blob.
        /// @return The number of bytes this property value occupies.
        flatbuffers::uoffset_t skip_property_value_at_offset(FgbColumnType col_type, const uint8_t* properties_data_start,
                                                             flatbuffers::uoffset_t value_offset_in_blob,
                                                             flatbuffers::uoffset_t properties_blob_size) const noexcept;

        /// @brief Retrieves the string value of a specific property for a feature by its column index.
        /// @param fbs_feature Pointer to the FlatBuffer Feature object.
        /// @param fbs_header Pointer to the FlatBuffer Header object (for column schema).
        /// @param target_column_index The index of the desired column/property.
        /// @return The property value as a string. Returns an empty string if not found or on error.
        std::string get_string_value_for_property(const FlatGeobuf::Feature* fbs_feature, const FlatGeobuf::Header* fbs_header,
                                                  std::size_t target_column_index) const noexcept(false);

        // Other Helpers
        /// @brief Finds the index of a property (column) by its name.
        /// @param fbs_header Pointer to the FlatBuffer Header object.
        /// @param property_name_to_find The name of the property to search for.
        /// @return An optional containing the index if found, otherwise `std::nullopt`.
        std::optional<std::size_t> find_property_index_by_name(const FlatGeobuf::Header* fbs_header,
                                                               std::string_view property_name_to_find) const noexcept;

        /// @brief Prints basic information from the FGB header to standard output.
        /// @param fbs_header Pointer to the FlatBuffer Header object.
        void print_header_info(const FlatGeobuf::Header* fbs_header) const noexcept;

        /// @brief Writes the CSV header row to the output file stream.
        /// @param out_file The output file stream.
        void write_csv_header(std::ofstream& out_file) const noexcept(false);

        /// @brief Writes a single CSV data row for a feature to the output file stream.
        /// @param out_file The output file stream.
        /// @param uat_name The UAT name for the feature.
        /// @param uat_code The UAT code for the feature (0 if not available).
        /// @param county_mn The 2-character county code for the feature.
        /// @param bbox The calculated bounding box for the feature.
        void write_csv_row(std::ofstream& out_file, const std::string& uat_name, std::uint32_t uat_code, const county_code& county_mn,
                           const bounding_box& bbox) const noexcept(false);

        /// @brief Loads the content of a file into a byte buffer.
        /// @param file_path The path to the file.
        /// @return A vector of bytes containing the file's content.
        std::vector<std::uint8_t> load_file_to_buffer(const std::string& file_path) const noexcept(false);

        /// @brief Static method to process a single feature; designed to be run in a separate thread.
        /// @param task_data Input data for the task, including geometry and identifiers.
        /// @return A `task_result` containing the identifiers and the calculated bounding box.
        static task_result process_single_feature_task(task_input_data task_data) noexcept;

        // Constants
        static constexpr int default_coordinate_stride_ {2};              /// Default stride (XY)
        static constexpr std::uint64_t progress_report_interval_ {1000u}; /// Interval for reporting progress
        static constexpr int csv_coordinate_precision {3};                /// Precision for coordinates in CSV output

        // Expected column names (assuming these are fixed for your specific FGB files)
        static constexpr std::string_view expected_uat_name_column_ {"name"};
        static constexpr std::string_view expected_uat_code_column_ {"natcode"};
        static constexpr std::string_view expected_county_mn_column_ {"countyMn"};

        static constexpr double square_meters_in_square_kilometer_ {1000000.0}; /// Conversion factor

        static constexpr char csv_delimiter_ {','};                                              /// CSV delimiter character.
        static constexpr std::string_view csv_newline_ {"\n"};                                   /// CSV newline string.
        static constexpr std::string_view uat_name_fallback_prefix_ {"Name_Unavailable_Index_"}; /// Prefix for fallback UAT names.
        static constexpr std::uint32_t min_fgb_file_size_ {12u}; /// Minimum valid FGB file size (8 magic + 4 header_size).

        // Member Variables
        const std::string input_fgb_path_;                            /// Path to the input FlatGeobuf file.
        const std::string output_csv_path_;                           /// Path to the output CSV file.
        thread_pool thread_pool_;                                     /// Thread pool for parallel processing.
        std::vector<std::uint8_t> fgb_buffer_ {};                     /// Buffer to hold the entire FGB file content.
        std::vector<std::future<task_result>> processing_futures_ {}; /// Futures for asynchronous task results.
        std::uint64_t feature_submission_count_ {};                   /// Counter for submitted features.

        /// Pre-calculated column index for the UAT name property.
        std::optional<std::size_t> uat_name_column_index_ {};
        /// Pre-calculated column index for the UAT code property.
        std::optional<std::size_t> uat_code_column_index_ {};
        /// Pre-calculated column index for the County MN property.
        std::optional<std::size_t> county_mn_column_index_ {};
    };

} // namespace kmx::gis
