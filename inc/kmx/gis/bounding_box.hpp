/// Copyright (c) 2025 - present KMX Systems. All rights reserved.
/// @file bounding_box.hpp
#pragma once
#ifndef PCH
    #include <iosfwd>
    #include <limits>
#endif

namespace kmx::gis
{
    /// @brief Represents a 2D bounding box defined by minimum and maximum coordinates.
    /// This struct is used to calculate and store the spatial extent of geometries.
    struct bounding_box
    {
        /// @brief Minimum X coordinate of the bounding box.
        double min_x {std::numeric_limits<double>::max()};
        /// @brief Minimum Y coordinate of the bounding box.
        double min_y {std::numeric_limits<double>::max()};
        /// @brief Maximum X coordinate of the bounding box.
        double max_x {};
        /// @brief Maximum Y coordinate of the bounding box.
        double max_y {};
        /// @brief Flag indicating whether the bounding box contains valid data.
        /// An invalid bounding box typically means it has not been updated with any coordinates.
        bool is_valid {};

        /// @brief String representation for an invalid bounding box when writing to CSV.
        static constexpr char const* invalid_bbox_csv_marker {",,,"};
        /// @brief Default precision used when writing coordinate values to a CSV stream.
        static constexpr int csv_coordinate_precision {3};

        /// @brief Default constructor.
        /// Initializes an invalid bounding box by setting coordinates to their respective extreme values
        /// (min to max double, max to lowest double) to ensure any valid point will correctly initialize the extent.
        bounding_box() noexcept = default;

        /// @brief Updates the bounding box to include a given point (x, y).
        /// If the bounding box is currently invalid, its extent is set to this point.
        /// Otherwise, the existing extent is expanded if necessary to include the point.
        /// @param x The X coordinate of the point to include.
        /// @param y The Y coordinate of the point to include.
        void update(const double x, const double y) noexcept;

        /// @brief Writes the bounding box coordinates to an output stream in CSV format.
        /// The output format is "min_x,min_y,max_x,max_y".
        /// If the bounding box is invalid (is_valid is false), it writes the `invalid_bbox_csv_marker`.
        /// @param os The output stream (e.g., std::ofstream) to write the formatted string to.
        /// @throws std::ios_base::failure On stream write errors if stream exceptions are enabled for `os`.
        void write_to_stream(std::ostream& os) const noexcept(false);
    };

} // namespace kmx::gis
