//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) {
    rows_ = rows;
    cols_ = cols;
  }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix() = default;
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    // automatically use the father construct function
    data_ = new T *[rows];
    for (int i = 0; i < rows; i++) {
      data_[i] = new T[cols];
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return this->rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return this->cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {
    // LOG_INFO("# i: %d \n", i);
    // LOG_INFO("# rows: %d \n", GetRowCount());
    // LOG_INFO("# j: %d \n", j);
    // LOG_INFO("# cols: %d \n", GetColumnCount());

    if (i >= 0 && i < GetRowCount() && j >= 0 && j < GetColumnCount()) {
      return data_[i][j];
    } else {
      throw OutOfRangeException("out of index range GetElement");
    }
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if (i >= 0 && i < GetRowCount() && j >= 0 && j < GetColumnCount()) {
      data_[i][j] = val;
      return;
    } else {
      throw OutOfRangeException("out of index range SetElement");
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    int source_size = source.size();
    if (source_size != this->rows_ * this->cols_) {
      throw OutOfRangeException("out of index range FillFrom");
      return;
    }
    for (int i = 0; i < this->rows_; i++) {
      for (int j = 0; j < this->cols_; j++) {
        data_[i][j] = source[i * this->cols_ + j];
      }
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  // rewrite the default thing
  ~RowMatrix() override {
    for (int i = 0; i < this->rows_; i++) {
      // classic way to delete the target code here
      delete[] data_[i];
    }
    delete[] data_;
    // free
  }

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    if (matrixA->GetRowCount() != matrixB->GetRowCount()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    if (matrixA->GetColumnCount() != matrixB->GetColumnCount()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    int rows = matrixA->GetRowCount();
    int cols = matrixA->GetColumnCount();
    if (rows <= 0 || cols <= 0) return std::unique_ptr<RowMatrix<T>>(nullptr);

    std::unique_ptr<RowMatrix<T>> res(new RowMatrix<T>(rows, cols));

    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        res->SetElement(i, j, matrixA->GetElement(i, j) + matrixB->GetElement(i, j));
      }
    }
    return res;
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */

  // damn complex for me to complete
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    int rows_A = matrixA->GetRowCount();
    int cols_A = matrixA->GetColumnCount();
    int rows_B = matrixB->GetRowCount();
    int cols_B = matrixB->GetColumnCount();

    if (cols_A != rows_B) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    if (rows_A <= 0 || cols_A <= 0 || rows_B <= 0 || cols_B <= 0) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    std::unique_ptr<RowMatrix<T>> res(new RowMatrix<T>(rows_A, cols_B));

    for (int i = 0; i < rows_A; i++) {
      for (int j = 0; j < cols_B; j++) {
        // easy mistake!
        T tmp = matrixA->GetElement(i, 0) * matrixB->GetElement(0, j);
        for (int m = 1; m < cols_A; m++) {
          tmp += matrixA->GetElement(i, m) * matrixB->GetElement(m, j);
        }
        res->SetElement(i, j, tmp);
      }
    }
    // complete the damn code!
    return res;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    // TODO(P0): Add implementation
    return std::unique_ptr<RowMatrix<T>>(nullptr);

    int rows_A = matrixA->GetRowCount();
    int cols_A = matrixA->GetColumnCount();
    int rows_B = matrixB->GetRowCount();
    int cols_B = matrixB->GetColumnCount();
    int rows_C = matrixC->GetRowCount();
    int cols_C = matrixC->GetColumnCount();

    if (cols_A != rows_B) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    if (rows_A <= 0 || cols_A <= 0 || rows_B <= 0 || cols_B <= 0 || rows_C <= 0 || cols_C <= 0) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    if (rows_C != rows_A || cols_C != cols_B) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    // why problrem?
    return Add(Multiply(matrixA, matrixB), matrixC);
  }
};
}  // namespace bustub