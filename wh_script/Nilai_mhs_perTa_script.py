import os, sys
sys.path.append('D://Magang/warehouse')
import base64
from pathlib import Path

import re
import pandas as pd
import numpy as np

from wh_script.utils.progress_info import use_progress_tracker

class NilaiMhsPerTA:
    def __init__(self, is_dev=False):

        #deklarasi query yang akan dipakai
        self.Q_KRS_AKTIF = '''
            select krs.id_jadwal, krs.nim, krs.kdmk, mk.nmmk, krs.sks, 
            (CASE
                WHEN krs.kdmk IN (Select kdmk from siakad.mk_penting) THEN "Ya"
                ELSE "Tidak"
            END) as is_tugas_akhir,
            jdwl.klpk, krs.ta,
            /*SUB QUERY NILAI HURUF*/
            (Select nakh from udinus.nilai_ujian
                where nim = krs.nim and kdmk = krs.kdmk and klpk = jdwl.klpk and ta = krs.ta
            ) as nilai_huruf,
            /*SUB QUERY NILAI BOBOT*/
            (Select score from udinus.nilai_ujian
                where nim = krs.nim and kdmk = krs.kdmk and klpk = jdwl.klpk and ta = krs.ta
            ) as bobot_nilai
            from siakad.krs_aktiv krs
            join siakad.mk_reg_view mk
            on krs.kdmk = mk.kdmk and left(mk.kur_nama,3) = left(krs.nim, 3)
            join siakad.krs_jadwal_aktiv jdwl
            on krs.id_jadwal = jdwl.id
            where krs.ta = {ta}
            and SUBSTR(krs.nim, 5,4) BETWEEN 2015 and 2021
            order by krs.nim asc;
        '''
        self.Q_KRS_ARCHIVE = '''
            select krs.id_jadwal, krs.nim, krs.kdmk, mk.nmmk, krs.sks, 
            (CASE
                WHEN krs.kdmk IN (Select kdmk from siakad.mk_ta) THEN "Ya"
                ELSE "Tidak"
            END) as is_tugas_akhir,
            jdwl.klpk, krs.ta, 
            /*SUB QUERY NILAI HURUF*/
            (Select nakh from udinus.nilai_ujian
                where nim = krs.nim and kdmk = krs.kdmk and klpk = jdwl.klpk and ta = krs.ta
            ) as nilai_huruf,
            /*SUB QUERY NILAI BOBOT*/
            (Select score from udinus.nilai_ujian
                where nim = krs.nim and kdmk = krs.kdmk and klpk = jdwl.klpk and ta = krs.ta
            ) as bobot_nilai
            from siakad.krs_archive krs
            join siakad.mk_reg_view mk
            on krs.kdmk = mk.kdmk and left(mk.kur_nama,3) = left(krs.nim, 3)
            join siakad.krs_jadwal_archive jdwl
            on krs.id_jadwal = jdwl.id
            where krs.ta = {ta}
            -- and SUBSTR(krs.nim, 5,4) BETWEEN 2015 and 2021
            order by krs.nim asc;
        '''
        self.Q_TA = '''
            select kode from siakad.tahun_ajaran
            where kode = %s
            and jns_smt != 0
            order by kode asc;
        '''
        self.Q_INSERT_DB = '''  
            INSERT INTO warehouse.wh_mahasiswa_nilai
            SET nim = %s, 
            kode_matkul = %s, nama_matkul = %s, jml_sks = %s,
            is_tugas_akhir = %s, kode_klpk = %s, periode = %s, 
            nilai_huruf = %s, bobot_nilai = %s;
        '''
        self.Q_GET_ID = '''
            SELECT * FROM warehouse.wh_mahasiswa_nilai
            WHERE nim = %s and periode = %s and kode_matkul = %s and kode_klpk = %s
            limit 1;
        '''
        self.Q_GET_ID_WH = '''
            SELECT * FROM warehouse.wh_mahasiswa_nilai;
        '''
        self.Q_UPDATE_DB = '''
            UPDATE warehouse.wh_mahasiswa_nilai
            SET kode_matkul = %s, nama_matkul = %s, jml_sks = %s,
            is_tugas_akhir = %s, kode_klpk = %s, periode = %s, 
            nilai_huruf = %s, bobot_nilai = %s
            WHERE nim = %s and periode = %s and kode_matkul = %s and kode_klpk = %s;
        '''

    def get_data_krs(self, curr, is_active, ta):
        cur = curr

        query_krs = self.Q_KRS_AKTIF if is_active else self.Q_KRS_ARCHIVE
        cur.execute(query_krs.format(ta=ta))
        columns = [desc[0] for desc in cur.description]
        
        result2 = cur.fetchall()
        dfKrs = pd.DataFrame(result2, columns=columns)

        return dfKrs

    def synchronize_to_db(self, curr, ta, conn, path):
        cur = curr
        cur.execute(self.Q_TA, (ta))
        columns = [desc[0] for desc in cur.description]
        resTa = cur.fetchall()
        dfTa = pd.DataFrame(resTa, columns=columns)

        for index, ta in dfTa.iterrows():
            
            dfKrs = pd.read_csv(path)
            dfKrs = dfKrs.astype(object).replace(np.nan, '')
            
            total_data = len(dfTa)
            i = 0
            (step, part, thresshold) = use_progress_tracker(total_data)
            print(f"Processing {total_data}")

            for index, item in enumerate(dfKrs.iterrows()):

                i+=1
                if i >= thresshold:
                    print(f"{i} from {total_data} : {i/total_data * 100}%")
                    step+=1
                    thresshold = part * step

                row = item[1]
                data = { 'nim': row['nim'], 
                'kode_matkul': row['kdmk'], 'nama_matkul': row['nmmk'], 
                'jml_sks': row['sks'],
                'is_tugas_akhir': row['is_tugas_akhir'], 'kode_klpk': row['klpk'], 
                'periode': row['ta'], 'nilai_huruf': row['nilai_huruf'],'bobot_nilai': row['bobot_nilai']
                }

                ### get id data ###
                rows_count = cur.execute(self.Q_GET_ID, (row['nim'], row['ta'], row['kdmk'], row['klpk']))
                res3 = cur.fetchall()

                if rows_count > 0:
                    cur.execute(self.Q_UPDATE_DB, (
                                            data['kode_matkul'], data['nama_matkul'], 
                                            data['jml_sks'],
                                            data['is_tugas_akhir'], data['kode_klpk'], 
                                            data['periode'], data['nilai_huruf'],
                                            data['bobot_nilai'],
                                            data['nim'], data['periode'], data['kode_matkul'], data['kode_klpk']
                                            )
                            )
                    conn.commit()
                else:
                    cur.execute(self.Q_INSERT_DB, (
                                            data['nim'], data['kode_matkul'], data['nama_matkul'], 
                                            data['jml_sks'],
                                            data['is_tugas_akhir'], data['kode_klpk'], 
                                            data['periode'], data['nilai_huruf'],
                                            data['bobot_nilai']
                                            )
                            )
                    conn.commit()

    def update_database(self, curr, conn, ta):
        #fungsi untuk mengupdate nilai, nama, jenjang, jurusan
        cur = curr

        sql_update = '''
            update warehouse.wh_mahasiswa_nilai nil
            join warehouse.wh_mahasiswa_pendaftaran d 
            on nil.nim = d.nim 
            set nil.nama = d.nama, 
            nil.kode_prodi = d.kode_prodi,
            nil.nama_prodi = d.nama_prodi,
            nil.jenjang = d.jenjang
            where nil.nim != '' and nil.periode = '{ta}';
        '''

        cur.execute(sql_update.format(ta=ta))
        conn.commit()


    def run(self, ta, is_active = False):
        
        try:
            self.get_data_krs()
        except Exception as e :
            self.logger.print_new_line("Error processing data: {e}", e=e)
            return
        
        try:
            self.synchronize_to_db()
        except KeyboardInterrupt:
            self.logger.print_new_line("KeyboardInterrupt: Menghentikan script")
            self.logger.update_log(self.synchronize_to_db,"Running", self.cursor_index_db)
            return
        
        try:
            self.update_database()
        except Exception as e :
            self.logger.print_new_line("Error processing data: {e}", e=e)
            return
        

# NilaiMhsPerTA(is_dev=True).run(20222)