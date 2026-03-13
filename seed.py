import sqlite3
con = sqlite3.connect('bot.db')
con.executemany(
    'INSERT OR IGNORE INTO services (name,price,duration,duration_min,img,sort_order,active) VALUES (?,?,?,?,?,?,1)',
    [
        ('Классический маникюр','15€','30 мин',30,'images/1.jpg',1),
        ('Гель-лак / Коррекция','25€','60 мин',60,'images/2.jpg',2),
        ('Наращивание ногтей','35€','120 мин',120,'images/3.jpg',3),
        ('Гигиенический педикюр','25€','45 мин',45,'images/4.jpg',4),
        ('Педикюр с гель-лаком','35€','60 мин',60,'images/5.jpg',5),
        ('Мужской педикюр','30€','60 мин',60,'images/6.jpg',6),
        ('Снятие покрытия','10€','15 мин',15,'images/7.jpg',7),
        ('Ремонт одного ногтя','2€','15 мин',15,'images/8.jpg',8),
    ]
)
con.commit()
print('Готово! Услуг в БД:', con.execute('SELECT COUNT(*) FROM services').fetchone()[0])
con.close()
