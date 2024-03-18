import math

def use_progress_tracker(total_data):
    step = 1
    part = math.floor(total_data * 0.2)
    thresshold = part * step
    return (step, part, thresshold)


# total_data = 1555
# (step, part, thresshold) = use_progress_tracker(total_data)
# for i in range(1,total_data+1):
#     if i >= thresshold:
#         print(f"{i} from {total_data} : {i/total_data * 100}%")
#         step+=1
#         thresshold = part * step