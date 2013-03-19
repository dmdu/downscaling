#!/usr/bin/env python
# a bar plot with errorbars
import numpy as np
import matplotlib.pyplot as plt

N = 4
ind = np.arange(N)  # the x locations for the groups
width = 0.70       # the width of the bars
palette = ['#D7D7D7','#B7B7B7','#6F6F6F','#6F6F6F']

fig = plt.figure()

# Workload Execution Time
wetMeans = (2.586, 2.914, 2.920, 2.969)
wetStd =   (0.002, 0.219, 0.090, 0.032)
ax = fig.add_subplot(131)
ax.set_ylabel('Workload execution time (hours)')
ax.set_xticks(ind+0.5*width)
ax.set_xticklabels( ('OI', 'FO', 'AP-25', 'AP-100'), rotation=90)
rects1 = ax.bar(ind, wetMeans, width, yerr=wetStd, ecolor='black', label='Opportunistic-Idle', color = palette, )
box1 = ax.get_position()
ax.set_position([box1.x0, box1.y0+box1.height*0.2 , box1.width*0.8, box1.height*0.7])
ax.set_ylim(0,4.0)

ax3 = fig.add_subplot(132)
# Convergence
cMeans = (3.129, 1.612, 2.453, 0.630)
cStd =   (0.035, 0.398, 0.088, 0.140)
ax3.set_ylabel('Convergence time (hours)')
ax3.set_xticks(ind+0.5*width)
ax3.set_xticklabels( ('OI', 'FO', 'AP-25', 'AP-100'), rotation=90)
rects3 = ax3.bar(ind, cMeans, width, yerr=cStd, ecolor='black', label='Aggressive', color = palette)
box3 = ax3.get_position()
ax3.set_position([box3.x0, box3.y0+box3.height*0.2 , box3.width*0.8, box3.height*0.7])
ax3.set_ylim(0,4.0)

ax2 = fig.add_subplot(133)
# Overhead
oMeans = (0.00001, 0.00001, 1.469, 5.759)
oStd =   (0, 0, 0.808, 0.750)
ax2.set_ylabel('Workload overhead (percent)')
ax2.set_xticks(ind+0.5*width)
ax2.set_xticklabels( ('OI', 'FO', 'AP-25', 'AP-100'), rotation=90)
rects2 = ax2.bar(ind, oMeans, width, yerr=oStd, ecolor='black', label='Opportunistic-Offline', color = palette)
box2 = ax2.get_position()
ax2.set_position([box2.x0 + box2.width*0.3, box2.y0+box2.height*0.2 , box2.width*0.8, box2.height*0.7])
ax2.set_ylim(0,8)


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

#def autolabel(rects):
#    # attach some text labels
#    for rect in rects:
#        height = rect.get_height()
        #ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height), ha='center', va='bottom')
        #print dir(rect)
#        rect.set_ec('#232323')
#autolabel(rects1)
#autolabel(rects2)
#autolabel(rects3)

#plt.show()
plt.savefig('overhead.png')