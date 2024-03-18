import base64
from pathlib import Path

import re
import pandas as pd
import numpy as np

from wh_script.utils.progress_info import use_progress_tracker

def q_data_yudisium(curr, wisuda_ke):
    cur = curr
    sql = '''
            SELECT nim, nama, LEFT(nim,3) AS kode_prodi, jurusan AS nama_prodi, jenjang, ta AS periode, tgl_yudisium, sk_yudisium,
            nril AS nirl, seri_no AS pin, sks, ipk, lama_studi AS masa_studi,
            predikat, kode_yud, wisuda_ke, kd.nomor_banpt AS no_sk_akreditasi_prodi, kd.stat_akred AS status_akreditasi_prodi
            FROM siakad.wisuda_calon 
            JOIN siakad.kode_fak AS kd
            ON LEFT(siakad.wisuda_calon.nim,3) = kd.kode
            WHERE wisuda_ke = {yud};
    '''

    cur.execute(sql.format(yud=wisuda_ke))
    result2 = cur.fetchall()

    columns = [desc[0] for desc in cur.description]

    dfKrs = pd.DataFrame(result2, columns=columns)
    return dfKrs
    
    # return dfKrs


def proses_ETL(curr, conn, wisuda_ke, path):
    cur = curr
    ### Query Insert tabel wh_mahasiswa_yudisium ###

    q_ta = '''
        select kode from siakad.tahun_ajaran
        where kode = %s
        and jns_smt != 0
        order by kode asc;
    '''

    q_insert = '''  
        INSERT INTO warehouse.wh_mahasiswa_yudisium
        SET nim = %s, nama = %s, kode_prodi = %s, nama_prodi = %s,
        jenjang = %s, periode = %s, sk_yudisium = %s, tgl_yudisium = %s,
        nirl = %s, pin = %s, sks = %s, ipk = %s, masa_studi = %s,
        predikat = %s, kode_yud = %s, wisuda_ke = %s, no_sk_akreditasi_prodi = %s,
        status_akreditasi_prodi = %s, no_sk_akreditasi_univ = %s, status_akreditasi_univ = %s;


    '''

    q_get_id = '''
        SELECT * FROM warehouse.wh_mahasiswa_yudisium
        WHERE nim = %s and periode = %s and kode_yud = %s
        limit 1;
    '''

    q_get_id_warehouse_all = '''
        SELECT * FROM warehouse.wh_mahasiswa_yudisium;
    '''

    q_update = '''
        UPDATE warehouse.wh_mahasiswa_yudisium
        SET nim = %s, nama = %s, kode_prodi = %s, nama_prodi = %s,
        jenjang = %s, periode = %s, sk_yudisium = %s, tgl_yudisium = %s,
        nirl = %s, pin = %s, sks = %s, ipk = %s, masa_studi = %s,
        predikat = %s, kode_yud = %s, wisuda_ke = %s, no_sk_akreditasi_prodi = %s,
        status_akreditasi_prodi = %s, no_sk_akreditasi_univ = %s, status_akreditasi_univ = %s
        WHERE nim = %s and periode = %s and kode_yud = %s;
    '''
#     cur.execute(q_ta, (periode))
#     resTa = cur.fetchall()

#     dfTa = pd.DataFrame(resTa)

    # nomer dan status akreditasi universitas saat ini
    no_akre_univ = '107/SK/BAN-PT/AK-ISK/PT/III/2022'
    status_akre_univ = 'UNGGUL'

    # for index, ta in dfTa.iterrows():
    dfYud = pd.read_csv(path)
    dfYud = dfYud.astype(object).replace(np.nan, '')

    total_data = len(dfYud)
    i = 0
    (step, part, thresshold) = use_progress_tracker(total_data)
    print(f"Processing {total_data}")

    for index, row in dfYud.iterrows():
        i+=1
        if i >= thresshold:
            print(f"{i} from {total_data} : {i/total_data * 100}%")
            step+=1
            thresshold = part * step

    # INSERT INTO warehouse.wh_mahasiswa_yudisium
    # SET nim = %s, nama = %s, kode_prodi = %s, nama_prodi = %s,
    # jenjang = %s, periode = %s, sk_yudisium = %s, tgl_yudisium = %s,
    # nirl = %s, pin = %s, sks = %s, ipk = %s, masa_studi = %s,
    # predikat = %s, kode_yud = %s, wisuda_ke = %s, no_sk_akreditasi_prodi = %s,
    # status_akreditasi_prodi = %s, no_sk_akreditasi_univ = %s, status_akreditasi_univ = %s;

        data = {   
                'nim' :   row['nim'],
                'nama' :   row['nama'],
                'kode_prodi' :   row['kode_prodi'],
                'nama_prodi' :   row['nama_prodi'],
                'jenjang' :   row['jenjang'],
                'periode' :   row['periode'],
                'sk_yudisium' :   row['sk_yudisium'],
                'tgl_yudisium' :   row['tgl_yudisium'],
                'nirl' :   row['nirl'],
                'pin' :   row['pin'],
                'sks' :   row['sks'],
                'ipk' :   row['ipk'],
                'masa_studi' :   row['masa_studi'],
                'predikat' :   row['predikat'],
                'kode_yud' :   row['kode_yud'],
                'wisuda_ke' :   row['wisuda_ke'],
                'no_sk_akreditasi_prodi' :   row['no_sk_akreditasi_prodi'],
                'status_akreditasi_prodi' :   row['status_akreditasi_prodi'],
                'no_sk_akreditasi_univ' :   no_akre_univ,
                'status_akreditasi_univ' :   status_akre_univ
                }
        # print(data)

        ### get id data ###
        rows_count = cur.execute(q_get_id, (row['nim'], row['periode'], row['kode_yud']))
        res3 = cur.fetchall()

        if rows_count > 0:
    #         print("Record", data['nim'],'|',data['periode'], "Sudah Ada")
            # print(index,' - ',data['nim'],'|',data['periode'],'|',data['kode_matkul'],'|',data['kode_klpk'] ,' - ','UPDATE')
            cur.execute(q_update, (
                                    data['nim'],
                                    data['nama'],
                                    data['kode_prodi'],
                                    data['nama_prodi'],
                                    data['jenjang'],
                                    data['periode'],
                                    data['sk_yudisium'],
                                    data['tgl_yudisium'],
                                    data['nirl'],
                                    data['pin'],
                                    data['sks'],
                                    data['ipk'],
                                    data['masa_studi'],
                                    data['predikat'],
                                    data['kode_yud'],
                                    data['wisuda_ke'],
                                    data['no_sk_akreditasi_prodi'],
                                    data['status_akreditasi_prodi'],
                                    data['no_sk_akreditasi_univ'],
                                    data['status_akreditasi_univ'],

                                    data['nim'], data['periode'], data['kode_yud']
                                    )
                       )
            conn.commit()

        else:
            # print(index,' - ',data['nim'],'|',data['periode'],'|',data['kode_matkul'],'|',data['kode_klpk'] ,' - ','INSERT')        
            cur.execute(q_insert, (
                                    data['nim'],
                                    data['nama'],
                                    data['kode_prodi'],
                                    data['nama_prodi'],
                                    data['jenjang'],
                                    data['periode'],
                                    data['sk_yudisium'],
                                    data['tgl_yudisium'],
                                    data['nirl'],
                                    data['pin'],
                                    data['sks'],
                                    data['ipk'],
                                    data['masa_studi'],
                                    data['predikat'],
                                    data['kode_yud'],
                                    data['wisuda_ke'],
                                    data['no_sk_akreditasi_prodi'],
                                    data['status_akreditasi_prodi'],
                                    data['no_sk_akreditasi_univ'],
                                    data['status_akreditasi_univ']

                                    )
                       )
            conn.commit()
        res1 = 'Periode wisuda ke ' + str(data['wisuda_ke']) + ' Selesai!'
    res2 = 'Alhamdulillah Bar Kabeh!'
    
    return(res1, res2)

def ETL_data_yud(wisuda_ke):
    
    ### Simpan data ke Excell dulu ###
    q_data_yudisium(wisuda_ke)
    
    ### Eksekusi ETL ###
    res = proses_ETL(wisuda_ke)
    
    return res

class YudisiumMhs:
    def get_data_yudisium(self, curr, wisuda_ke):
        print(wisuda_ke)
        print(type(wisuda_ke))
        return q_data_yudisium(curr, wisuda_ke)
    
    def load_data(self, curr, conn, wisuda_ke, path):
        return proses_ETL(curr, conn, wisuda_ke, path)