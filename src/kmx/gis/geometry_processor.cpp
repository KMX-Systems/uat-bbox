/// Copyright (c) 2025 - present KMX Systems. All rights reserved.
/// @file geometry_processor.cpp
#include "kmx/gis/geometry_processor.hpp"
#include "flatgeobuf/feature_generated.h" // For FlatGeobuf::Geometry, FlatGeobuf::GeometryType
#include "flatgeobuf/header_generated.h"  // For FlatGeobuf::GeometryType enum

namespace kmx::gis
{
    // Updates a given bounding box with coordinates from a simple geometry's coordinate array.
    void geometry_processor::update_bbox_from_coordinates(bounding_box& bb, const FlatGeobuf::Geometry& geom_fbs,
                                                          const std::uint32_t stride) noexcept
    {
        const auto* const coords_vector = geom_fbs.xy();
        // Check if coordinates vector is null or empty
        if ((coords_vector == nullptr) || (coords_vector->size() == 0u))
            return;

        const double* const coords_data {coords_vector->data()};
        const std::size_t num_doubles {coords_vector->size()};

        // Iterate through coordinate pairs (or triplets, etc., based on stride)
        for (std::size_t i {}; (i + 1u) < num_doubles; i += stride)
            bb.update(coords_data[i], coords_data[i + 1u]); // Assumes XY are the first two components
    }

    // Processes a FlatBuffer Geometry object that represents a single polygon, including its rings.
    void geometry_processor::process_single_polygon_for_bbox(bounding_box& bbox, const FlatGeobuf::Geometry* const polygon_fbs,
                                                             const std::uint32_t coordinate_stride) noexcept
    {
        // Check if the polygon geometry pointer is valid
        if (polygon_fbs != nullptr)
        {
            // A Polygon's parts are its rings. Each ring is also a FlatGeobuf::Geometry (like a LineString).
            const auto* const rings = polygon_fbs->parts();
            if (rings != nullptr) // Check if the polygon has parts (rings)
                // Iterate through the rings (parts) of the polygon
                for (flatbuffers::uoffset_t i {}; i < rings->size(); ++i)
                {
                    const FlatGeobuf::Geometry* const ring_fbs = rings->Get(i);           // Get the geometry for the current ring
                    if (ring_fbs != nullptr)                                              // Check if the ring geometry is valid
                        update_bbox_from_coordinates(bbox, *ring_fbs, coordinate_stride); // Update bbox with ring coordinates
                }
            // This case handles a Polygon with only an exterior ring, where coordinates are directly
            // on the Polygon object and it has no explicit "parts" table for its single ring.
            else if ((polygon_fbs->xy() != nullptr) && (polygon_fbs->xy()->size() > 0u))
                update_bbox_from_coordinates(bbox, *polygon_fbs, coordinate_stride); // Update bbox with polygon's own coordinates
        }
    }

    // Calculates the bounding box for a given FlatBuffer Geometry object.
    bounding_box geometry_processor::calculate_for_geometry(const FlatGeobuf::Geometry* const geometry_fbs_table,
                                                            const std::uint32_t coordinate_stride,
                                                            const FlatGeobuf::GeometryType actual_geometry_type) noexcept
    {
        bounding_box bbox {}; // Initialize an empty, invalid bounding box
        // Return invalid bbox if no geometry is provided
        if (geometry_fbs_table == nullptr)
            return bbox;

        switch (actual_geometry_type)
        {
            case FlatGeobuf::GeometryType::Polygon:
                // The top-level geometry is a Polygon. Process its rings.
                process_single_polygon_for_bbox(bbox, geometry_fbs_table, coordinate_stride);
                break;
            case FlatGeobuf::GeometryType::MultiPolygon:
                // The top-level geometry is a MultiPolygon. Its parts are individual Polygons.
                { // Block for local variable 'polygon_parts'
                    const auto* const polygon_parts = geometry_fbs_table->parts();
                    if (polygon_parts != nullptr) // Check if the MultiPolygon has parts
                        // Iterate through the individual Polygons within the MultiPolygon
                        for (flatbuffers::uoffset_t i {}; i < polygon_parts->size(); ++i)
                        {
                            const FlatGeobuf::Geometry* const single_polygon_fbs = polygon_parts->Get(i); // Get a single Polygon
                            if (single_polygon_fbs != nullptr) // Check if the Polygon part is valid
                                // Process this single_polygon_fbs as a Polygon (which will handle its rings)
                                process_single_polygon_for_bbox(bbox, single_polygon_fbs, coordinate_stride);
                        }
                }
                break;
            default:
                // This is a fallback for simple, non-collection types like Point or LineString,
                // or if a Polygon/MultiPolygon somehow ended up here without parts (unlikely for valid FGB).
                if ((geometry_fbs_table->xy() != nullptr) && (geometry_fbs_table->xy()->size() > 0u))
                    update_bbox_from_coordinates(bbox, *geometry_fbs_table, coordinate_stride);
                break;
        }

        // Note: Other geometry types (e.g., Point, LineString, MultiPoint, MultiLineString)
        // would need specific handling if this function were to be made more generic.
        return bbox;
    }

} // namespace kmx::gis
