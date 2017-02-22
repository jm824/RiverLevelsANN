import matplotlib.pyplot as plt

x = [1,2,3]
y = [6,4,9]

x2 = [1,2,7]
y2 = [4,8,6]

plt.plot(x,y, label='historic readings')
plt.plot(x2,y2, label='predicted readings')
plt.xlabel('datetime')
plt.ylabel('water level')
plt.title('Historic readings vs predicted readings')

plt.show()