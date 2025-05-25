/// Copyright (c) 2025 - present KMX Systems. All rights reserved.
/// @file bounding_box.cpp
#include "kmx/gis/bounding_box.hpp"
#include <iomanip>
#include <iostream>

namespace kmx::gis
{
    void bounding_box::update(const double x, const double y) noexcept
    {
        min_x = std::min(min_x, x);
        min_y = std::min(min_y, y);
        max_x = std::max(max_x, x);
        max_y = std::max(max_y, y);
        is_valid = true;
    }

    void bounding_box::write_to_stream(std::ostream& os) const noexcept(false)
    {
        if (!is_valid)
        {
            os << invalid_bbox_csv_marker;
            return;
        }

        const std::ios_base::fmtflags original_flags {os.flags()};
        const std::streamsize original_precision {os.precision()};

        os << std::fixed << std::setprecision(csv_coordinate_precision) << min_x << "," << min_y << "," << max_x << "," << max_y;

        os.flags(original_flags);
        os.precision(original_precision);
    }

} // namespace kmx::gis
