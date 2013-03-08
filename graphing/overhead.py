#!/usr/bin/env python
# a bar plot with errorbars
import numpy as np
import matplotlib.pyplot as plt

N = 4
ind = np.arange(N)  # the x locations for the groups
width = 0.70       # the width of the bars
palette = ['#B3B3B3','#8D8D8D','#232323','#232323']

fig = plt.figure()

# Workload exec time
oiMeans = (2.586, 2.958, 3.534, 2.958)
#oiStd =   (2, 3, 5, 6)
ax = fig.add_subplot(131)
ax.set_ylabel('Workload execution time, hours')
ax.set_xticks(ind+0.5*width)
ax.set_xticklabels( ('OI', 'FO', 'AP-15m', 'AP-Inf'), rotation=90)
#rects1 = ax.bar(ind, oiMeans, width, yerr=oiStd, label='Opportunistic-Idle', color = palette)
rects1 = ax.bar(ind, oiMeans, width, label='Opportunistic-Idle', color = palette)
box = ax.get_position()
ax.set_position([box.x0, box.y0+box.height*0.1 , box.width*0.8, box.height*0.9])
ax.set_ylim(0,4.5)

ax2 = fig.add_subplot(132)
# Overhead
ooMeans = (0.00001, 0.00001, 0.234, 5.421)
#ooStd =   (3, 5, 3, 4)
ax2.set_ylabel('Workload overhead, %')
ax2.set_xticks(ind+0.5*width)
ax2.set_xticklabels( ('OI', 'FO', 'AP-15m', 'AP-Inf'), rotation=90)
rects2 = ax2.bar(ind, ooMeans, width, label='Opportunistic-Offline', color = palette)
box = ax2.get_position()
ax2.set_position([box.x0, box.y0+box.height*0.1 , box.width*0.8, box.height*0.9])
ax2.set_ylim(0,10)

ax3 = fig.add_subplot(133)
# C0nvergence
apMeans = (3.170, 3.312, 2.563, 0.579)
#apStd =   (3, 5, 10, 4)
ax3.set_ylabel('Convergence time, hours')
ax3.set_xticks(ind+0.5*width)
ax3.set_xticklabels( ('OI', 'FO', 'AP-15m', 'AP-Inf'), rotation=90)
rects3 = ax3.bar(ind, apMeans, width, label='Aggressive', color = palette)
box = ax3.get_position()
ax3.set_position([box.x0, box.y0+box.height*0.1 , box.width*0.8, box.height*0.9])
ax3.set_ylim(0,4.5)


# add some
#ax.set_ylabel('Overhead, %')
#ax.set_title('Execution time overhead by policy and workload')
#ax.set_xticks(ind+1.5*width)
#ax.set_xticklabels( ('Workload A', 'Workload B') )

#ax.legend( (rects1[0], rects2[0], rects3[0]), ('Opportunistic-Idle', 'Opportunistic-Offline', 'Aggressive') )

# Shink current axis's height by 10% on the bottom
#box = ax.get_position()
#ax.set_position([box.x0, box.y0 + box.height * 0.15,
#                 box.width, box.height * 0.8])

# Put a legend below current axis
#ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1),
#          fancybox=False, shadow=False, ncol=3)

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