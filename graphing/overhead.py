#!/usr/bin/env python
# a bar plot with errorbars
import numpy as np
import matplotlib.pyplot as plt

N = 2
oiMeans = (20, 35)
oiStd =   (2, 3)

ind = np.arange(2)  # the x locations for the groups
print ind

width = 0.20       # the width of the bars

fig = plt.figure()
ax = fig.add_subplot(111)
rects1 = ax.bar(ind, oiMeans, width, color='g', yerr=oiStd, label='Opportunistic-Idle')

ooMeans = (25, 32)
ooStd =   (3, 5)
rects2 = ax.bar(ind+width, ooMeans, width, color='y', yerr=ooStd, label='Opportunistic-Offline')

apMeans = (27, 34)
apStd =   (3, 5)
rects3 = ax.bar(ind+2*width, apMeans, width, color='r', yerr=apStd, label='Aggressive')

# add some
ax.set_ylabel('Overhead, %')
ax.set_title('Execution time overhead by policy and workload')
ax.set_xticks(ind+1.5*width)
ax.set_xticklabels( ('Workload A', 'Workload B') )

#ax.legend( (rects1[0], rects2[0], rects3[0]), ('Opportunistic-Idle', 'Opportunistic-Offline', 'Aggressive') )

# Shink current axis's height by 10% on the bottom
box = ax.get_position()
ax.set_position([box.x0, box.y0 + box.height * 0.15,
                 box.width, box.height * 0.8])

# Put a legend below current axis
ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1),
          fancybox=False, shadow=False, ncol=3)

# def autolabel(rects):
#     # attach some text labels
#     for rect in rects:
#         height = rect.get_height()
#         ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
#                ha='center', va='bottom')

#autolabel(rects1)
#autolabel(rects2)
#autolabel(rects3)

#plt.show()
plt.savefig('overhead.png')