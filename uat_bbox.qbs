CppApplication {
    consoleApplication: true
    cpp.cxxLanguageVersion: "c++23"
    cpp.enableRtti: false
    install: true
    cpp.includePaths: [
        "inc",
        "inc_dep"
    ]
    files: [
        "inc/kmx/gis/bounding_box.hpp",
        "inc/kmx/gis/flatgeobuf_processor.hpp",
        "inc/kmx/gis/geometry_processor.hpp",
        "inc/kmx/gis/types.hpp",
        "inc/kmx/thread_pool.hpp",
        "src/flatgeobuf/packedrtree.cpp",
        "src/kmx/gis/bunding_box.cpp",
        "src/kmx/gis/flatgeobuf_processor.cpp",
        "src/kmx/gis/geometry_processor.cpp",
        "src/kmx/gis/main.cpp",
        "src/kmx/thread_pool.cpp",
    ]
}
