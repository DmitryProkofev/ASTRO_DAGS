nums = [5,7,3,9,4,9,8,3,1]

# res = []
# for el in nums:
#     s = nums.count(el)
#     if s > 0:
#         res.append(el)

# print(max(res))

counts = {}
res_list = []
for el in nums:
    res = counts.get(el, 1)
    counts[res] = el


print(counts)