set(PROTO_DIR "${CMAKE_SOURCE_DIR}/proto")
set(PROTO_FILE "${PROTO_DIR}/service.proto")

set(PROTO_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/service.pb.cc" "${CMAKE_CURRENT_SOURCE_DIR}/service.grpc.pb.cc")
set(PROTO_HDRS "${CMAKE_CURRENT_SOURCE_DIR}/service.pb.h" "${CMAKE_CURRENT_SOURCE_DIR}/service.grpc.pb.h")
set_source_files_properties(${PROTO_SRCS} ${PROTO_HDRS} PROPERTIES GENERATED TRUE)

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
    COMMAND $<TARGET_FILE:protobuf::protoc>
            --grpc_out=${CMAKE_CURRENT_SOURCE_DIR}
            --cpp_out=${CMAKE_CURRENT_SOURCE_DIR}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
            -I ${PROTO_DIR}
            ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    VERBATIM
)

add_library(db_api STATIC ${PROTO_SRCS})
target_link_libraries(db_api PUBLIC db_project_headers gRPC::grpc++ protobuf::libprotobuf)

target_include_directories(db_api PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_custom_target(generate_proto DEPENDS ${PROTO_SRCS})
