import pymysql
import time

#Database Toko
conn_toko = pymysql.connect(host='remotemysql.com', user='MUIsHiFNT1', passwd='yB2s5qaB0D', db='MUIsHiFNT1')
#Database Bank
conn_bank = pymysql.connect(host='remotemysql.com', user='Vc7dm7hG1J', passwd='wXuiW8eXFu', db='Vc7dm7hG1J')
# conn_toko = pymysql.connect(host='us-cdbr-gcp-east-01.cleardb.net', user='b19032ffc105e0', passwd='f344a622', db='gcp_2b3c1115685a234c7c42')
# conn_bank = pymysql.connect(host='us-cdbr-gcp-east-01.cleardb.net', user='b226152072b3d7', passwd='a183ec8e', db='gcp_af82b207b6e1e17125a2')
# conn_toko = pymysql.connect(host='192.168.1.1', user='atk', passwd='12345678', db='db_toko')
# conn_bank = pymysql.connect(host='192.168.1.2', user='adi', passwd='12345678', db='db_bank')


cur_toko = conn_toko.cursor()
cur_bank = conn_bank.cursor()

def engineToko():
    #Query select all
    select_transaksi="""SELECT * FROM tb_transaksi;"""
    select_integrasi="""SELECT * FROM tb_integrasi;"""

    #mengembil data pada tabel tb_transaksi
    cur_toko.execute(select_transaksi)
    data_toko = cur_toko.fetchall()
    conn_toko.commit()

    #mengembil data pada tabel tb_integrasi
    cur_toko.execute(select_integrasi)
    data_integrasi = cur_toko.fetchall()
    conn_toko.commit()

    #sinkronisasi data tb_transaksi dengan tb_integrasi
    if len(data_toko) > len(data_integrasi):
        #menampilkan data apa yang baru ditambahkan pada tb_transaksi
        data_beda = [item for item in data_toko if item not in data_integrasi]
        for data in data_beda:
            cur_toko.execute("""INSERT INTO tb_integrasi(id_integrasi,id_pembeli,total_transaksi,tgl_transaksi,status)
                            VALUES(%s,%s,%s,%s,%s)""",(data[0],data[1],data[2],data[3],data[4]))
            conn_toko.commit()
            print("DATA",data_beda,"BERHASIL DI UPDATE pada db_toko.tb_integrasi!")

    elif len(data_integrasi) > len(data_toko):
        data_beda=[item for item in data_integrasi if item not in data_toko]
        print("DATA",data_beda,"TERHAPUS PADA db_toko.tb_transaksi")#menampilkan data apa yang di hapus pada tb_transaksi
        for data in data_beda:
            #menghapus data pada tb_integrasi yang tidak terdapat pada tb_transaksi
            cur_toko.execute("""DELETE FROM tb_integrasi WHERE id_integrasi = %s """,data[0])
            conn_toko.commit()
            print("DATA",data_beda,"BERHASIL DIHAPUS PADA db_toko.tb_integrasi !!!")

    elif len (data_toko) == len(data_integrasi):
        data_beda=[item for item in data_toko if item not in data_integrasi]
        for data in data_beda:
            print("DATA BEDA:",data_beda)
            cur_toko.execute(
                """UPDATE tb_integrasi SET id_integrasi=%s , id_pembeli =%s , total_transaksi = %s , tgl_transaksi = %s , status = %s WHERE id_integrasi=%s"""
                , (data[0], data[1], data[2], data[3], data[4],data[0]))
            conn_toko.commit()
            print("DATA",data_beda,"BERHASIL DI SESUAIKAN DENGAN db_toko.tb_transaksi")

        else:
                print("TIDAK ADA DATA BARU ATAU DATA BEDA PADA db_toko")

def engineSinkronisasi():
    select_integrasi = """SELECT * FROM tb_integrasi"""
    #data integrasi pada toko
    cur_toko.execute(select_integrasi)
    integrasi_toko = cur_toko.fetchall()
    conn_toko.commit()

    #data integrasi pada bank
    select_integrasi1 = """SELECT * FROM tb_integrasi"""
    cur_bank.execute(select_integrasi1)
    integrasi_bank = cur_bank.fetchall()
    conn_bank.commit()


    if len(integrasi_toko) > len(integrasi_bank):
        data_beda = [item for item in integrasi_toko if item not in integrasi_bank]
        for data in data_beda:
            cur_bank.execute("""INSERT INTO tb_integrasi
                            VALUES (%s,%s,%s,%s,%s)""",(data[0],data[1],data[2],data[3],data[4]))
            conn_bank.commit() #untuk menyimpan perubahan data pada db_bank
            print("DATA",data_beda,"BERHASIL DI UPDATE PADA db_bank.tb_integrasi!!!")

    elif len(integrasi_bank) > len(integrasi_toko):
        data_beda=[item for item in integrasi_bank if item not in integrasi_toko]
        for data in data_beda:
            cur_bank.execute("""DELETE FROM tb_integrasi WHERE id_integrasi = %s""",data[0])
            conn_bank.commit()
            print("DATA",data_beda,"BERHASIL DIHAPUS PADA db_bank.tb_integrasi JUGA!")

    elif len(integrasi_bank) == len(integrasi_toko):
        cur_bank.execute("SELECT * FROM tb_integrasi")
        sql_bank = cur_bank.fetchall()
        conn_bank.commit()

        cur_toko.execute("SELECT * FROM tb_integrasi")
        sql_toko = cur_toko.fetchall()
        conn_toko.commit()

        data_beda=[item for item in sql_toko if item not in sql_bank]
        for data in data_beda:
                print("Data Beda:",data_beda)
                cur_bank.execute("""UPDATE tb_integrasi SET id_integrasi=%s,id_pembeli=%s,total_transaksi=%s,tgl_transaksi=%s,status=%s WHERE id_integrasi=%s; """,(data[0],data[1],data[2],data[3],data[4],data[0]))
                conn_bank.commit()
                print("DATA",data_beda,"BERHASIL DI SESUAIKAN DENGAN db_toko.tb_integrasi")


        else:
            print("TIDAK ADA DATA BARU ATAU DATA BEDA PADA db_toko.tb_integrasi")

def engineBank():
    select_integrasi="""SELECT * FROM tb_integrasi"""
    select_toko="""SELECT * FROM tb_transaksi"""

    cur_bank.execute(select_integrasi)
    sql_bank = cur_bank.fetchall()
    conn_bank.commit()

    cur_toko.execute(select_toko)
    sql_toko=cur_toko.fetchall()
    conn_toko.commit()


    data_beda=[item for item in sql_bank if item not in sql_toko]
    for data in data_beda:
        cur_toko.execute("""UPDATE tb_integrasi SET status=%s WHERE id_integrasi=%s; """,(data[4],data[0]))
        conn_toko.commit()
        print("STATUS PADA id_integrasi=",data[0],"TELAH DIPERBAHARUI MENJADI '1'")

        cur_toko.execute("""UPDATE tb_transaksi SET status=%s WHERE id_transaksi=%s; """, (data[4], data[0]))
        conn_toko.commit()
        print("STATUS PADA id_transaksi=", data[0], "TELAH DIPERBAHARUI MENJADI '1'")


while 1 :#perulangan terus menerus
    try :

        try:
            engineBank() #memanggil engine bank
        except Exception as e:
            print(e)

        try:
            engineToko() #memanggil engine toko
        except Exception as e:
            print(e)

        try:
            engineSinkronisasi() #memanggil engine sinkronisasi
        except Exception as e:
            print(e)



    except:
        print("ERROR!")
    time.sleep(15) #memberikan time delay manual selama 15 detik
