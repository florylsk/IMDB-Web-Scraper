f1data = f2data = f3data= f4data = f5data = f6data = f7data = f8data =""
with open('movies_p1.json', encoding='utf-8') as f1:
  f1data = f1.read()

with open('movies_p2.json', encoding='utf-8') as f2:
  f2data = f2.read()
with open('movies_p3.json', encoding='utf-8') as f3:
  f3data = f3.read()
with open('movies_p4.json', encoding='utf-8') as f4:
  f4data = f4.read()
with open('movies_p5.json', encoding='utf-8') as f5:
  f5data = f5.read()
with open('movies_p6.json', encoding='utf-8') as f6:
  f6data = f6.read()
with open('movies_p7.json', encoding='utf-8') as f7:
  f7data = f7.read()
with open('movies_p8.json', encoding='utf-8') as f8:
  f8data = f8.read()

f1data += "\n"
f1data += f2data
f1data += "\n"
f1data += f3data
f1data += "\n"
f1data += f4data
f1data += "\n"
f1data += f5data
f1data += "\n"
f1data += f6data
f1data += "\n"
f1data += f7data
f1data += "\n"
f1data += f8data

with open('movies_final.json', 'w', encoding='utf-8') as f:
  f.write(f1data)