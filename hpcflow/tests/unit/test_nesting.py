# import pytest
# from hpcflow.sdk.core.nesting import NestingView, NestingSequence


# def test_sorted_by_nesting_order():
#     seqs = [
#         NestingSequence(path="A", length=1, nesting_order=0),
#         NestingSequence(path="B", length=1, nesting_order=1),
#     ]
#     nv = NestingView(*seqs)
#     assert nv.paths == ("A", "B")
#     assert nv.num_groups == 2
#     assert nv.groups[0].sequences == [seqs[0]]
#     assert nv.groups[1].sequences == [seqs[1]]

#     nv = NestingView(*seqs[::-1])
#     assert nv.paths == ("A", "B")
#     assert nv.num_groups == 2
#     assert nv.groups[0].sequences == [seqs[0]]
#     assert nv.groups[1].sequences == [seqs[1]]


# def test_get_indices_simple_nesting():
#     nv = NestingView(
#         NestingSequence(path="A", length=2, nesting_order=0),
#         NestingSequence(path="B", length=3, nesting_order=1),
#     )
#     assert nv.paths == ("A", "B")
#     assert nv.get_indices(as_list=True) == [
#         (0, 0),
#         (0, 1),
#         (0, 2),
#         (1, 0),
#         (1, 1),
#         (1, 2),
#     ]

#     assert nv.get_indices((2, 3, 1), as_list=True) == [
#         (0, 2),
#         (1, 0),
#         (0, 1),
#     ]


# def test_get_indices_simple_zipping():
#     nv = NestingView(
#         NestingSequence(path="A", length=3, nesting_order=1),
#         NestingSequence(path="B", length=3, nesting_order=1),
#     )
#     assert nv.paths == ("A", "B")
#     assert nv.get_indices(as_list=True) == [
#         (0, 0),
#         (1, 1),
#         (2, 2),
#     ]
#     assert nv.get_indices((2, 0), as_list=True) == [
#         (2, 2),
#         (0, 0),
#     ]


# def test_get_indices_nesting_and_zipping_and_non_integer():
#     nv = NestingView(
#         NestingSequence(path="A", length=3, nesting_order=0),
#         NestingSequence(path="B", length=3, nesting_order=0),
#         NestingSequence(path="C", length=2, nesting_order=1),
#         NestingSequence(path="D", length=6, nesting_order=1.5),
#         NestingSequence(path="E", length=3, nesting_order=2),
#         NestingSequence(path="F", length=4, nesting_order=3),
#         NestingSequence(path="G", length=2, nesting_order=4),
#     )
#     assert nv.paths == ("A", "B", "C", "D", "E", "F", "G")
#     indices = nv.get_indices(as_list=True)
#     assert len(indices) == nv.num_items == (3 * 2 * 3 * 4 * 2)
#     assert all(len(row) == nv.num_sequences == 7 for row in indices)
#     assert indices == [
#         (0, 0, 0, 0, 0, 0, 0),
#         (0, 0, 0, 0, 0, 0, 1),
#         (0, 0, 0, 0, 0, 1, 0),
#         (0, 0, 0, 0, 0, 1, 1),
#         (0, 0, 0, 0, 0, 2, 0),
#         (0, 0, 0, 0, 0, 2, 1),
#         (0, 0, 0, 0, 0, 3, 0),
#         (0, 0, 0, 0, 0, 3, 1),
#         (0, 0, 0, 0, 1, 0, 0),
#         (0, 0, 0, 0, 1, 0, 1),
#         (0, 0, 0, 0, 1, 1, 0),
#         (0, 0, 0, 0, 1, 1, 1),
#         (0, 0, 0, 0, 1, 2, 0),
#         (0, 0, 0, 0, 1, 2, 1),
#         (0, 0, 0, 0, 1, 3, 0),
#         (0, 0, 0, 0, 1, 3, 1),
#         (0, 0, 0, 0, 2, 0, 0),
#         (0, 0, 0, 0, 2, 0, 1),
#         (0, 0, 0, 0, 2, 1, 0),
#         (0, 0, 0, 0, 2, 1, 1),
#         (0, 0, 0, 0, 2, 2, 0),
#         (0, 0, 0, 0, 2, 2, 1),
#         (0, 0, 0, 0, 2, 3, 0),
#         (0, 0, 0, 0, 2, 3, 1),
#         (0, 0, 1, 1, 0, 0, 0),
#         (0, 0, 1, 1, 0, 0, 1),
#         (0, 0, 1, 1, 0, 1, 0),
#         (0, 0, 1, 1, 0, 1, 1),
#         (0, 0, 1, 1, 0, 2, 0),
#         (0, 0, 1, 1, 0, 2, 1),
#         (0, 0, 1, 1, 0, 3, 0),
#         (0, 0, 1, 1, 0, 3, 1),
#         (0, 0, 1, 1, 1, 0, 0),
#         (0, 0, 1, 1, 1, 0, 1),
#         (0, 0, 1, 1, 1, 1, 0),
#         (0, 0, 1, 1, 1, 1, 1),
#         (0, 0, 1, 1, 1, 2, 0),
#         (0, 0, 1, 1, 1, 2, 1),
#         (0, 0, 1, 1, 1, 3, 0),
#         (0, 0, 1, 1, 1, 3, 1),
#         (0, 0, 1, 1, 2, 0, 0),
#         (0, 0, 1, 1, 2, 0, 1),
#         (0, 0, 1, 1, 2, 1, 0),
#         (0, 0, 1, 1, 2, 1, 1),
#         (0, 0, 1, 1, 2, 2, 0),
#         (0, 0, 1, 1, 2, 2, 1),
#         (0, 0, 1, 1, 2, 3, 0),
#         (0, 0, 1, 1, 2, 3, 1),
#         (1, 1, 0, 2, 0, 0, 0),
#         (1, 1, 0, 2, 0, 0, 1),
#         (1, 1, 0, 2, 0, 1, 0),
#         (1, 1, 0, 2, 0, 1, 1),
#         (1, 1, 0, 2, 0, 2, 0),
#         (1, 1, 0, 2, 0, 2, 1),
#         (1, 1, 0, 2, 0, 3, 0),
#         (1, 1, 0, 2, 0, 3, 1),
#         (1, 1, 0, 2, 1, 0, 0),
#         (1, 1, 0, 2, 1, 0, 1),
#         (1, 1, 0, 2, 1, 1, 0),
#         (1, 1, 0, 2, 1, 1, 1),
#         (1, 1, 0, 2, 1, 2, 0),
#         (1, 1, 0, 2, 1, 2, 1),
#         (1, 1, 0, 2, 1, 3, 0),
#         (1, 1, 0, 2, 1, 3, 1),
#         (1, 1, 0, 2, 2, 0, 0),
#         (1, 1, 0, 2, 2, 0, 1),
#         (1, 1, 0, 2, 2, 1, 0),
#         (1, 1, 0, 2, 2, 1, 1),
#         (1, 1, 0, 2, 2, 2, 0),
#         (1, 1, 0, 2, 2, 2, 1),
#         (1, 1, 0, 2, 2, 3, 0),
#         (1, 1, 0, 2, 2, 3, 1),
#         (1, 1, 1, 3, 0, 0, 0),
#         (1, 1, 1, 3, 0, 0, 1),
#         (1, 1, 1, 3, 0, 1, 0),
#         (1, 1, 1, 3, 0, 1, 1),
#         (1, 1, 1, 3, 0, 2, 0),
#         (1, 1, 1, 3, 0, 2, 1),
#         (1, 1, 1, 3, 0, 3, 0),
#         (1, 1, 1, 3, 0, 3, 1),
#         (1, 1, 1, 3, 1, 0, 0),
#         (1, 1, 1, 3, 1, 0, 1),
#         (1, 1, 1, 3, 1, 1, 0),
#         (1, 1, 1, 3, 1, 1, 1),
#         (1, 1, 1, 3, 1, 2, 0),
#         (1, 1, 1, 3, 1, 2, 1),
#         (1, 1, 1, 3, 1, 3, 0),
#         (1, 1, 1, 3, 1, 3, 1),
#         (1, 1, 1, 3, 2, 0, 0),
#         (1, 1, 1, 3, 2, 0, 1),
#         (1, 1, 1, 3, 2, 1, 0),
#         (1, 1, 1, 3, 2, 1, 1),
#         (1, 1, 1, 3, 2, 2, 0),
#         (1, 1, 1, 3, 2, 2, 1),
#         (1, 1, 1, 3, 2, 3, 0),
#         (1, 1, 1, 3, 2, 3, 1),
#         (2, 2, 0, 4, 0, 0, 0),
#         (2, 2, 0, 4, 0, 0, 1),
#         (2, 2, 0, 4, 0, 1, 0),
#         (2, 2, 0, 4, 0, 1, 1),
#         (2, 2, 0, 4, 0, 2, 0),
#         (2, 2, 0, 4, 0, 2, 1),
#         (2, 2, 0, 4, 0, 3, 0),
#         (2, 2, 0, 4, 0, 3, 1),
#         (2, 2, 0, 4, 1, 0, 0),
#         (2, 2, 0, 4, 1, 0, 1),
#         (2, 2, 0, 4, 1, 1, 0),
#         (2, 2, 0, 4, 1, 1, 1),
#         (2, 2, 0, 4, 1, 2, 0),
#         (2, 2, 0, 4, 1, 2, 1),
#         (2, 2, 0, 4, 1, 3, 0),
#         (2, 2, 0, 4, 1, 3, 1),
#         (2, 2, 0, 4, 2, 0, 0),
#         (2, 2, 0, 4, 2, 0, 1),
#         (2, 2, 0, 4, 2, 1, 0),
#         (2, 2, 0, 4, 2, 1, 1),
#         (2, 2, 0, 4, 2, 2, 0),
#         (2, 2, 0, 4, 2, 2, 1),
#         (2, 2, 0, 4, 2, 3, 0),
#         (2, 2, 0, 4, 2, 3, 1),
#         (2, 2, 1, 5, 0, 0, 0),
#         (2, 2, 1, 5, 0, 0, 1),
#         (2, 2, 1, 5, 0, 1, 0),
#         (2, 2, 1, 5, 0, 1, 1),
#         (2, 2, 1, 5, 0, 2, 0),
#         (2, 2, 1, 5, 0, 2, 1),
#         (2, 2, 1, 5, 0, 3, 0),
#         (2, 2, 1, 5, 0, 3, 1),
#         (2, 2, 1, 5, 1, 0, 0),
#         (2, 2, 1, 5, 1, 0, 1),
#         (2, 2, 1, 5, 1, 1, 0),
#         (2, 2, 1, 5, 1, 1, 1),
#         (2, 2, 1, 5, 1, 2, 0),
#         (2, 2, 1, 5, 1, 2, 1),
#         (2, 2, 1, 5, 1, 3, 0),
#         (2, 2, 1, 5, 1, 3, 1),
#         (2, 2, 1, 5, 2, 0, 0),
#         (2, 2, 1, 5, 2, 0, 1),
#         (2, 2, 1, 5, 2, 1, 0),
#         (2, 2, 1, 5, 2, 1, 1),
#         (2, 2, 1, 5, 2, 2, 0),
#         (2, 2, 1, 5, 2, 2, 1),
#         (2, 2, 1, 5, 2, 3, 0),
#         (2, 2, 1, 5, 2, 3, 1),
#     ]


# def test_get_indices_nesting_and_zipping_and_multiple_non_integer():
#     nv = NestingView(
#         NestingSequence(path="A", length=3, nesting_order=0),
#         NestingSequence(path="C", length=2, nesting_order=1),
#         NestingSequence(path="D", length=6, nesting_order=1.5),
#         NestingSequence(path="E", length=3, nesting_order=2),
#         NestingSequence(path="F", length=4, nesting_order=3),
#         NestingSequence(path="B", length=3, nesting_order=0),
#         NestingSequence(path="G", length=72, nesting_order=3.5),
#         NestingSequence(path="H", length=2, nesting_order=4),
#     )
#     assert nv.paths == ("A", "B", "C", "D", "E", "F", "G", "H")
#     indices = nv.get_indices(as_list=True)
#     assert len(indices) == nv.num_items == (3 * 2 * 3 * 4 * 2)
#     assert all(len(row) == nv.num_sequences == 8 for row in indices)
#     assert indices == [
#         (0, 0, 0, 0, 0, 0, 0, 0),
#         (0, 0, 0, 0, 0, 0, 0, 1),
#         (0, 0, 0, 0, 0, 1, 1, 0),
#         (0, 0, 0, 0, 0, 1, 1, 1),
#         (0, 0, 0, 0, 0, 2, 2, 0),
#         (0, 0, 0, 0, 0, 2, 2, 1),
#         (0, 0, 0, 0, 0, 3, 3, 0),
#         (0, 0, 0, 0, 0, 3, 3, 1),
#         (0, 0, 0, 0, 1, 0, 4, 0),
#         (0, 0, 0, 0, 1, 0, 4, 1),
#         (0, 0, 0, 0, 1, 1, 5, 0),
#         (0, 0, 0, 0, 1, 1, 5, 1),
#         (0, 0, 0, 0, 1, 2, 6, 0),
#         (0, 0, 0, 0, 1, 2, 6, 1),
#         (0, 0, 0, 0, 1, 3, 7, 0),
#         (0, 0, 0, 0, 1, 3, 7, 1),
#         (0, 0, 0, 0, 2, 0, 8, 0),
#         (0, 0, 0, 0, 2, 0, 8, 1),
#         (0, 0, 0, 0, 2, 1, 9, 0),
#         (0, 0, 0, 0, 2, 1, 9, 1),
#         (0, 0, 0, 0, 2, 2, 10, 0),
#         (0, 0, 0, 0, 2, 2, 10, 1),
#         (0, 0, 0, 0, 2, 3, 11, 0),
#         (0, 0, 0, 0, 2, 3, 11, 1),
#         (0, 0, 1, 1, 0, 0, 12, 0),
#         (0, 0, 1, 1, 0, 0, 12, 1),
#         (0, 0, 1, 1, 0, 1, 13, 0),
#         (0, 0, 1, 1, 0, 1, 13, 1),
#         (0, 0, 1, 1, 0, 2, 14, 0),
#         (0, 0, 1, 1, 0, 2, 14, 1),
#         (0, 0, 1, 1, 0, 3, 15, 0),
#         (0, 0, 1, 1, 0, 3, 15, 1),
#         (0, 0, 1, 1, 1, 0, 16, 0),
#         (0, 0, 1, 1, 1, 0, 16, 1),
#         (0, 0, 1, 1, 1, 1, 17, 0),
#         (0, 0, 1, 1, 1, 1, 17, 1),
#         (0, 0, 1, 1, 1, 2, 18, 0),
#         (0, 0, 1, 1, 1, 2, 18, 1),
#         (0, 0, 1, 1, 1, 3, 19, 0),
#         (0, 0, 1, 1, 1, 3, 19, 1),
#         (0, 0, 1, 1, 2, 0, 20, 0),
#         (0, 0, 1, 1, 2, 0, 20, 1),
#         (0, 0, 1, 1, 2, 1, 21, 0),
#         (0, 0, 1, 1, 2, 1, 21, 1),
#         (0, 0, 1, 1, 2, 2, 22, 0),
#         (0, 0, 1, 1, 2, 2, 22, 1),
#         (0, 0, 1, 1, 2, 3, 23, 0),
#         (0, 0, 1, 1, 2, 3, 23, 1),
#         (1, 1, 0, 2, 0, 0, 24, 0),
#         (1, 1, 0, 2, 0, 0, 24, 1),
#         (1, 1, 0, 2, 0, 1, 25, 0),
#         (1, 1, 0, 2, 0, 1, 25, 1),
#         (1, 1, 0, 2, 0, 2, 26, 0),
#         (1, 1, 0, 2, 0, 2, 26, 1),
#         (1, 1, 0, 2, 0, 3, 27, 0),
#         (1, 1, 0, 2, 0, 3, 27, 1),
#         (1, 1, 0, 2, 1, 0, 28, 0),
#         (1, 1, 0, 2, 1, 0, 28, 1),
#         (1, 1, 0, 2, 1, 1, 29, 0),
#         (1, 1, 0, 2, 1, 1, 29, 1),
#         (1, 1, 0, 2, 1, 2, 30, 0),
#         (1, 1, 0, 2, 1, 2, 30, 1),
#         (1, 1, 0, 2, 1, 3, 31, 0),
#         (1, 1, 0, 2, 1, 3, 31, 1),
#         (1, 1, 0, 2, 2, 0, 32, 0),
#         (1, 1, 0, 2, 2, 0, 32, 1),
#         (1, 1, 0, 2, 2, 1, 33, 0),
#         (1, 1, 0, 2, 2, 1, 33, 1),
#         (1, 1, 0, 2, 2, 2, 34, 0),
#         (1, 1, 0, 2, 2, 2, 34, 1),
#         (1, 1, 0, 2, 2, 3, 35, 0),
#         (1, 1, 0, 2, 2, 3, 35, 1),
#         (1, 1, 1, 3, 0, 0, 36, 0),
#         (1, 1, 1, 3, 0, 0, 36, 1),
#         (1, 1, 1, 3, 0, 1, 37, 0),
#         (1, 1, 1, 3, 0, 1, 37, 1),
#         (1, 1, 1, 3, 0, 2, 38, 0),
#         (1, 1, 1, 3, 0, 2, 38, 1),
#         (1, 1, 1, 3, 0, 3, 39, 0),
#         (1, 1, 1, 3, 0, 3, 39, 1),
#         (1, 1, 1, 3, 1, 0, 40, 0),
#         (1, 1, 1, 3, 1, 0, 40, 1),
#         (1, 1, 1, 3, 1, 1, 41, 0),
#         (1, 1, 1, 3, 1, 1, 41, 1),
#         (1, 1, 1, 3, 1, 2, 42, 0),
#         (1, 1, 1, 3, 1, 2, 42, 1),
#         (1, 1, 1, 3, 1, 3, 43, 0),
#         (1, 1, 1, 3, 1, 3, 43, 1),
#         (1, 1, 1, 3, 2, 0, 44, 0),
#         (1, 1, 1, 3, 2, 0, 44, 1),
#         (1, 1, 1, 3, 2, 1, 45, 0),
#         (1, 1, 1, 3, 2, 1, 45, 1),
#         (1, 1, 1, 3, 2, 2, 46, 0),
#         (1, 1, 1, 3, 2, 2, 46, 1),
#         (1, 1, 1, 3, 2, 3, 47, 0),
#         (1, 1, 1, 3, 2, 3, 47, 1),
#         (2, 2, 0, 4, 0, 0, 48, 0),
#         (2, 2, 0, 4, 0, 0, 48, 1),
#         (2, 2, 0, 4, 0, 1, 49, 0),
#         (2, 2, 0, 4, 0, 1, 49, 1),
#         (2, 2, 0, 4, 0, 2, 50, 0),
#         (2, 2, 0, 4, 0, 2, 50, 1),
#         (2, 2, 0, 4, 0, 3, 51, 0),
#         (2, 2, 0, 4, 0, 3, 51, 1),
#         (2, 2, 0, 4, 1, 0, 52, 0),
#         (2, 2, 0, 4, 1, 0, 52, 1),
#         (2, 2, 0, 4, 1, 1, 53, 0),
#         (2, 2, 0, 4, 1, 1, 53, 1),
#         (2, 2, 0, 4, 1, 2, 54, 0),
#         (2, 2, 0, 4, 1, 2, 54, 1),
#         (2, 2, 0, 4, 1, 3, 55, 0),
#         (2, 2, 0, 4, 1, 3, 55, 1),
#         (2, 2, 0, 4, 2, 0, 56, 0),
#         (2, 2, 0, 4, 2, 0, 56, 1),
#         (2, 2, 0, 4, 2, 1, 57, 0),
#         (2, 2, 0, 4, 2, 1, 57, 1),
#         (2, 2, 0, 4, 2, 2, 58, 0),
#         (2, 2, 0, 4, 2, 2, 58, 1),
#         (2, 2, 0, 4, 2, 3, 59, 0),
#         (2, 2, 0, 4, 2, 3, 59, 1),
#         (2, 2, 1, 5, 0, 0, 60, 0),
#         (2, 2, 1, 5, 0, 0, 60, 1),
#         (2, 2, 1, 5, 0, 1, 61, 0),
#         (2, 2, 1, 5, 0, 1, 61, 1),
#         (2, 2, 1, 5, 0, 2, 62, 0),
#         (2, 2, 1, 5, 0, 2, 62, 1),
#         (2, 2, 1, 5, 0, 3, 63, 0),
#         (2, 2, 1, 5, 0, 3, 63, 1),
#         (2, 2, 1, 5, 1, 0, 64, 0),
#         (2, 2, 1, 5, 1, 0, 64, 1),
#         (2, 2, 1, 5, 1, 1, 65, 0),
#         (2, 2, 1, 5, 1, 1, 65, 1),
#         (2, 2, 1, 5, 1, 2, 66, 0),
#         (2, 2, 1, 5, 1, 2, 66, 1),
#         (2, 2, 1, 5, 1, 3, 67, 0),
#         (2, 2, 1, 5, 1, 3, 67, 1),
#         (2, 2, 1, 5, 2, 0, 68, 0),
#         (2, 2, 1, 5, 2, 0, 68, 1),
#         (2, 2, 1, 5, 2, 1, 69, 0),
#         (2, 2, 1, 5, 2, 1, 69, 1),
#         (2, 2, 1, 5, 2, 2, 70, 0),
#         (2, 2, 1, 5, 2, 2, 70, 1),
#         (2, 2, 1, 5, 2, 3, 71, 0),
#         (2, 2, 1, 5, 2, 3, 71, 1),
#     ]


# def test_get_indices_two_equal_non_integer():
#     nv = NestingView(
#         NestingSequence(path="A", length=3, nesting_order=0),
#         NestingSequence(path="B", length=3, nesting_order=0.5),
#         NestingSequence(path="C", length=3, nesting_order=0.5),
#     )
#     assert nv.paths == ("A", "B", "C")
#     assert nv.num_groups == 2
#     assert nv.get_indices(as_list=True) == [
#         (0, 0, 0),
#         (1, 1, 1),
#         (2, 2, 2),
#     ]


# def test_adjacent_non_integer_grouped_with_tolerance():
#     nv_1 = NestingView(
#         NestingSequence(path="A", length=1, nesting_order=0),
#         NestingSequence(path="B", length=2, nesting_order=0 + NestingView._TOL * 0.1),
#     )
#     assert nv_1.paths == ("A", "B")
#     assert nv_1.num_groups == 1

#     with pytest.raises(ValueError):
#         nv_2 = NestingView(
#             NestingSequence(path="A", length=1, nesting_order=0),
#             NestingSequence(path="B", length=2, nesting_order=0 + NestingView._TOL * 10),
#         )


# def test_nested_len_prods():
#     nv = NestingView(
#         NestingSequence(path="A", length=2, nesting_order=0),
#         NestingSequence(path="B", length=3, nesting_order=1),
#     )
#     assert nv.paths == ("A", "B")
#     assert nv.nested_lengths == (2, 3, 1)
#     assert nv.nested_len_fwd_prods == (6, 3, 1)
#     assert nv.nested_len_bwd_prods == (2, 6)


# def test_raise_on_zero_length():
#     with pytest.raises(ValueError):
#         NestingView(NestingSequence(path="A", length=0, nesting_order=0))


# def test_raise_on_duplicate_path():
#     with pytest.raises(ValueError):
#         NestingView(
#             NestingSequence(path="A", length=1, nesting_order=0),
#             NestingSequence(path="A", length=2, nesting_order=0),
#         )


# def test_raise_on_non_integer_first():
#     """Non-integer nesting orders must have something to merge into, so this should not be
#     allowed."""
#     with pytest.raises(ValueError):
#         NestingView(NestingSequence(path="A", length=6, nesting_order=0.5))


# def test_raise_on_non_integer_bad_length():
#     with pytest.raises(ValueError):
#         NestingView(
#             NestingSequence(path="A", length=6, nesting_order=0),
#             NestingSequence(path="B", length=2, nesting_order=0.5),  # should be length 6
#         )
