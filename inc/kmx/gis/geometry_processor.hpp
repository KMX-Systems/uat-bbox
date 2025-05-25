/// Copyright (c) 2025 - present KMX Systems. All rights reserved.
/// @file geometry_processor.hpp
#pragma once
#ifndef PCH
    #include "kmx/gis/types.hpp"
#endif

// Forward declarations for FlatGeobuf types
namespace FlatGeobuf
{
    class Geometry;
    enum class GeometryType : uint8_t;
}

namespace kmx::gis
{
    /// @brief A stateless utility class for processing FlatGeobuf geometries to calculate their bounding boxes.
    /// This class encapsulates the logic for iterating through potentially complex geometry structures
    /// by directly using the `parts()` accessor from FlatBuffer-generated `FlatGeobuf::Geometry` objects.
    class geometry_processor
    {
    public:
        /// @brief Calculates the bounding box for a given FlatBuffer Geometry object.
        /// This method is the main entry point for geometry processing within this class.
        /// It manually iterates through geometry parts for Polygons and MultiPolygons.
        /// @param geometry_fbs Pointer to the constant FlatBuffer Geometry table. If null, an invalid bounding box is returned.
        /// @param coordinate_stride The number of `double` values per coordinate point (e.g., 2 for XY, 3 for XYZ). Must be positive.
        /// @param actual_geometry_type The specific `FlatGeobuf::GeometryType` of the `geometry_fbs` provided.
        /// @return A `bounding_box` representing the calculated extent. The `is_valid` flag of the
        ///         returned box will indicate if a valid extent was computed.
        static bounding_box calculate_for_geometry(const FlatGeobuf::Geometry* geometry_fbs, std::uint32_t coordinate_stride,
                                                   FlatGeobuf::GeometryType actual_geometry_type) noexcept;

    private:
        /// @brief Updates a given bounding box with coordinates from a simple geometry's coordinate array.
        /// A "simple" geometry here typically refers to a part that directly contains an array of coordinates,
        /// such as a linestring, a polygon ring, or a set of points.
        /// @param bb The `bounding_box` object to be updated (passed by reference).
        /// @param geom_fbs The constant FlatBuffer Geometry table from which to read coordinates (via `geom_fbs.xy()`).
        /// @param stride The number of `double` values per coordinate point, used to correctly step through the `xy` array.
        static void update_bbox_from_coordinates(bounding_box& bb, const FlatGeobuf::Geometry& geom_fbs, std::uint32_t stride) noexcept;

        /// @brief Processes a FlatBuffer Geometry object that represents a single polygon, including its rings.
        /// It iterates through the polygon's rings (parts) and updates the bounding box.
        /// @param bbox The `bounding_box` to update.
        /// @param polygon_fbs Pointer to the constant FlatBuffer Geometry table representing the polygon.
        /// @param coordinate_stride The coordinate stride.
        static void process_single_polygon_for_bbox(bounding_box& bbox, const FlatGeobuf::Geometry* polygon_fbs,
                                                    std::uint32_t coordinate_stride) noexcept;
    };

} // namespace kmx::gis
