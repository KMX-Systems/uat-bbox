/// Copyright (c) 2025 - present KMX Systems. All rights reserved.
/// @file types.hpp
#pragma once
#ifndef PCH
    #include "kmx/gis/bounding_box.hpp"
    #include <cstdint>
    #include <string>
#endif

// Forward declare FlatGeobuf types to minimize header dependencies
namespace FlatGeobuf
{
    enum class GeometryType : std::uint8_t;
    class Geometry;
}

namespace kmx::gis
{
    /// @brief Represents a 2-character county code using snake_case convention.
    struct county_code
    {
        char code[2u]; /// Stores the 2 characters of the county code.

        /// @brief Default constructor, initializes with spaces as placeholders.
        county_code(): code {' ', ' '} {}

        /// @brief Constructor from a string. Takes the first 2 chars.
        /// If the string is shorter than 2 characters, it pads with spaces.
        /// If the string is empty, the code remains spaces.
        /// @param s The input string, expected to contain the county code.
        explicit county_code(const std::string& s): code {' ', ' '}
        {
            if (!s.empty())
            {
                code[0] = s[0];
                if (s.length() >= 2)
                    code[1] = s[1];
                // else code[1] remains a space if s.length() == 1
            }
        }

        /// @brief Converts the 2-character code to a std::string.
        /// @return A std::string representation of the code. Returns an empty string
        ///         if the code consists of placeholder spaces.
        std::string to_string() const
        {
            // Return empty string if it's the placeholder, otherwise the 2 chars.
            // This helps in writing an empty field to CSV if the code is effectively "missing".
            if (is_empty())
            {
                return {};
            }

            return std::string(code, 2);
        }

        /// @brief Checks if the code is effectively empty (consists of placeholder spaces).
        /// @return True if the code is composed of placeholder spaces, false otherwise.
        bool is_empty() const { return (code[0u] == ' ') && (code[1u] == ' '); }

        /// @brief Equality comparison operator.
        /// @param other The other county_code object to compare against.
        /// @return True if both codes are identical, false otherwise.
        bool operator==(const county_code& other) const { return (code[0u] == other.code[0u]) && (code[1u] == other.code[1u]); }
        /// @brief Inequality comparison operator.
        /// @param other The other county_code object to compare against.
        /// @return True if the codes are different, false otherwise.
        bool operator!=(const county_code& other) const { return !(*this == other); }
    };

    /// @brief Holds input data required for a parallel task that calculates a feature's bounding box.
    /// This structure is designed to be passed by value or moved into a task.
    struct task_input_data
    {
        /// @brief The name of the UAT (Unitate Administrativ Teritorială).
        std::string uat_name {};
        /// @brief The code of the UAT (e.g., SIRUTA code). Defaults to 0 if not found.
        std::uint32_t uat_code {};
        /// @brief The county identifier (e.g., "SJ", "BH"). Defaults to placeholder if not found.
        county_code county_mn {};
        /// @brief A pointer to the constant FlatBuffer Geometry table associated with the feature.
        /// This points to data within the main file buffer and should not outlive it.
        const FlatGeobuf::Geometry* geometry_ptr {};
        /// @brief The number of double values that constitute a single coordinate point (e.g., 2 for XY, 3 for XYZ).
        std::uint32_t coordinate_stride {};
        /// @brief The specific geometry type of this feature or feature part, as defined by `FlatGeobuf::GeometryType`.
        FlatGeobuf::GeometryType actual_geometry_type {};
    };

    /// @brief Holds the result produced by a parallel bounding box calculation task.
    /// This structure is returned from a task and contains the feature's UAT name, UAT code, county code, and its computed bounding box.
    struct task_result
    {
        /// @brief The name of the UAT, corresponding to `task_input_data::uat_name`.
        std::string uat_name {};
        /// @brief The code of the UAT, corresponding to `task_input_data::uat_code`. Defaults to 0 if not found.
        std::uint32_t uat_code {};
        /// @brief The county identifier, corresponding to `task_input_data::county_mn`.
        county_code county_mn {};
        /// @brief The calculated `bounding_box` for the feature's geometry.
        bounding_box bbox {};
    };

} // namespace kmx::gis
