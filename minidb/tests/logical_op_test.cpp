#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "minidb/logical_op.hpp"

namespace {

minidb::ExprPtr identifier(std::vector<std::string> parts) {
  return std::make_shared<minidb::Expr>(minidb::Expr{
      .node =
          minidb::IdentifierExpr{
              .name = minidb::Identifier{.parts = std::move(parts)},
          },
  });
}

minidb::ExprPtr numeric_literal(std::string_view text) {
  return std::make_shared<minidb::Expr>(minidb::Expr{
      .node =
          minidb::LiteralExpr{
              .value = minidb::NumericLiteral{.text = std::string(text)},
          },
  });
}

minidb::ExprPtr boolean_literal(bool value) {
  return std::make_shared<minidb::Expr>(minidb::Expr{
      .node =
          minidb::LiteralExpr{
              .value = minidb::BooleanLiteral{.value = value},
          },
  });
}

minidb::ExprPtr binary(minidb::ExprPtr left, minidb::BinaryOperator op,
                       minidb::ExprPtr right) {
  return std::make_shared<minidb::Expr>(minidb::Expr{
      .node =
          minidb::BinaryExpr{
              .left = std::move(left),
              .op = op,
              .right = std::move(right),
          },
  });
}

std::uint32_t ref_index(const minidb::ExprPtr &expr) {
  return std::get<minidb::BoundRefExpr>(expr->node).ref.index;
}

minidb::TableSchema users_schema() {
  return minidb::TableSchema{
      .name = "users",
      .storage_name = "t_users",
      .next_index_storage_id = 0,
      .comment = std::nullopt,
      .columns =
          {
              minidb::ColumnSchema{
                  .name = "id",
                  .type = minidb::ColumnType::Integer,
                  .type_size = std::nullopt,
                  .is_nullable = false,
                  .is_unique = true,
                  .is_primary_key = true,
                  .comment = std::nullopt,
              },
              minidb::ColumnSchema{
                  .name = "name",
                  .type = minidb::ColumnType::Char,
                  .type_size = 32,
                  .is_nullable = false,
                  .is_unique = false,
                  .is_primary_key = false,
                  .comment = std::nullopt,
              },
              minidb::ColumnSchema{
                  .name = "age",
                  .type = minidb::ColumnType::Integer,
                  .type_size = std::nullopt,
                  .is_nullable = false,
                  .is_unique = false,
                  .is_primary_key = false,
                  .comment = std::nullopt,
              },
              minidb::ColumnSchema{
                  .name = "enabled",
                  .type = minidb::ColumnType::Boolean,
                  .type_size = std::nullopt,
                  .is_nullable = false,
                  .is_unique = false,
                  .is_primary_key = false,
                  .comment = std::nullopt,
              },
          },
      .indexes = {},
  };
}

} // namespace

TEST_CASE("logical operators bind identifiers to input slots") {
  // SELECT users.id, name, age + 10
  // FROM users
  // WHERE enabled = TRUE
  // ORDER BY 3 ASC, name DESC;
  minidb::Table users(users_schema(), std::filesystem::path{"unused"});

  auto scan = std::make_shared<minidb::ScanLogicalOp>(users);
  auto filter = std::make_shared<minidb::FilterLogicalOp>(
      scan, binary(identifier({"enabled"}), minidb::BinaryOperator::Equal,
                   boolean_literal(true)));

  minidb::FrameDescriptor project_frame{
      minidb::FrameColumnDescriptor{
          .name = "id",
          // FROM has resolved "users", so users.id binds to the scan's id slot.
          .expr = identifier({"users", "id"}),
          .visible = true,
      },
      minidb::FrameColumnDescriptor{
          .name = "name",
          .expr = identifier({"name"}),
          .visible = true,
      },
      minidb::FrameColumnDescriptor{
          .name = "age + 10",
          .expr = binary(identifier({"age"}), minidb::BinaryOperator::Add,
                         numeric_literal("10")),
          .visible = true,
      },
  };
  auto project =
      std::make_shared<minidb::ProjectLogicalOp>(filter, project_frame);

  std::vector<minidb::OrderByTerm> order_by{
      minidb::OrderByTerm{
          .key = std::uint32_t{3},
          .direction = minidb::SortDirection::Asc,
      },
      minidb::OrderByTerm{
          .key = identifier({"name"}),
          .direction = minidb::SortDirection::Desc,
      },
      minidb::OrderByTerm{
          .key = binary(identifier({"age"}), minidb::BinaryOperator::Add,
                        numeric_literal("10")),
          .direction = minidb::SortDirection::Asc,
      },
      minidb::OrderByTerm{
          .key = identifier({"enabled"}),
          .direction = minidb::SortDirection::Asc,
      },
      minidb::OrderByTerm{
          .key = identifier({"enabled"}),
          .direction = minidb::SortDirection::Desc,
      },
  };
  auto sort = std::make_shared<minidb::SortLogicalOp>(project, order_by);

  const auto result_frame = sort->get_frame();
  REQUIRE(result_frame.size() == 4);
  REQUIRE(result_frame[0].name == "id");
  REQUIRE(ref_index(result_frame[0].expr) == 1);
  REQUIRE(result_frame[1].name == "name");
  REQUIRE(ref_index(result_frame[1].expr) == 2);

  const auto &age_plus_ten =
      std::get<minidb::BinaryExpr>(result_frame[2].expr->node);
  REQUIRE(age_plus_ten.op == minidb::BinaryOperator::Add);
  REQUIRE(ref_index(age_plus_ten.left) == 3);
  REQUIRE(result_frame[2].visible);
  REQUIRE(result_frame[3].name.empty());
  REQUIRE(ref_index(result_frame[3].expr) == 0);
  REQUIRE_FALSE(result_frame[3].visible);

  const auto scan_frame = scan->get_frame();
  REQUIRE(scan_frame.size() == 4);
  REQUIRE(scan_frame[0].name == "enabled");
  REQUIRE(ref_index(scan_frame[0].expr) == 3);
  REQUIRE(scan_frame[1].name == "id");
  REQUIRE(ref_index(scan_frame[1].expr) == 0);
  REQUIRE(scan_frame[2].name == "name");
  REQUIRE(ref_index(scan_frame[2].expr) == 1);
  REQUIRE(scan_frame[3].name == "age");
  REQUIRE(ref_index(scan_frame[3].expr) == 2);
}

TEST_CASE("logical operator binding rejects unknown identifiers") {
  minidb::Table users(users_schema(), std::filesystem::path{"unused"});
  auto scan = std::make_shared<minidb::ScanLogicalOp>(users);

  REQUIRE_THROWS_AS(minidb::FilterLogicalOp(scan, identifier({"missing"})),
                    minidb::BinderError);
}
