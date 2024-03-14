import os
import pandas as pd
import numpy as np

'''
    Nama tabel untuk dosen pengajar 
    1. Production = warehouse.wh_dosen_pengajar
    2. Development = tampung.wh_dosen_pengajar_luthfi
'''
VAR_NAMA_TABEL_DOSEN_WH = 'tampung.wh_dosen_pengajar_luthfi'

class PengajaranDosenPerTA:
    def __init__(self):
        # Deklarasi query yang akan di pakai
        self.Q_GET_DOSEN_ACTIVE = '''
            select * from (
                select 
                (CASE
                    WHEN jdwl.kdds2 != 0 THEN (SELECT nidn FROM simpeg.tb_01 where staf_id = jdwl.kdds2 and nidn!='' limit 1)
                    ELSE (SELECT nidn FROM simpeg.tb_01 where staf_id = jdwl.kdds and nidn!='' limit 1)
                END) as nidn, 
                (CASE
                    WHEN jdwl.kdds2 != 0 THEN (SELECT trim(concat(gdp, ' ', nama, ', ', gdb)) FROM simpeg.tb_01 where staf_id = jdwl.kdds2 and nidn!='' limit 1)
                    ELSE (SELECT trim(concat(gdp, ' ', nama, ', ', gdb)) FROM simpeg.tb_01 where staf_id = jdwl.kdds and nidn!='' limit 1)
                END) as nama, 
                jdwl.id as id_jadwal,
                jdwl.kdmk, 
                mk.nmmk, mk.sks, jdwl.klpk, mk.kur_nama, 
                (CASE 
                    WHEN left(klpk,1) != 'U' THEN (select kode from siakad.kode_fak where kode = left(klpk,3))
                    WHEN mk.nmmk like '%pancasila%' THEN (select kode from siakad.kode_fak where kode = 'A11')
                    WHEN mk.nmmk like '%kewarganegaraan%' THEN (select kode from siakad.kode_fak where kode = 'A12')
                    ELSE '' 
                END) as kode_prodi
                ,
                (CASE
                    WHEN left(klpk,1) != 'U' THEN (select jurusan from siakad.kode_fak where kode = left(klpk,3))
                    WHEN mk.nmmk like '%pancasila%' THEN (select jurusan from siakad.kode_fak where kode = 'A11')
                    WHEN mk.nmmk like '%kewarganegaraan%' THEN (select jurusan from siakad.kode_fak where kode = 'A12')
                    ELSE ''
                END) as nama_prodi
                ,jdwl.ta
                from siakad.krs_jadwal_aktiv jdwl
                join siakad.mk_reg_view mk 
                on jdwl.kdmk = mk.kdmk
            ) rs 
            where rs.kode_prodi = left(rs.kur_nama,3)
        '''
        self.Q_GET_DOSEN_ARCHIVE = '''
            select * from (
                select 
                (CASE
                    WHEN jdwl.kdd2 != 0 THEN (SELECT nidn FROM simpeg.tb_01 where staf_id = jdwl.kdd2 and nidn!='' limit 1)
                    ELSE (SELECT nidn FROM simpeg.tb_01 where staf_id = jdwl.kdds and nidn!='' limit 1)
                END) as nidn, 
                (CASE
                    WHEN jdwl.kdd2 != 0 THEN (SELECT trim(concat(gdp, ' ', nama, ', ', gdb)) FROM simpeg.tb_01 where staf_id = jdwl.kdd2 and nidn!='' limit 1)
                    ELSE (SELECT trim(concat(gdp, ' ', nama, ', ', gdb)) FROM simpeg.tb_01 where staf_id = jdwl.kdds and nidn!='' limit 1)
                END) as nama, 
                jdwl.id as id_jadwal,
                jdwl.kdmk, 
                mk.nmmk, mk.sks, jdwl.klpk, mk.kur_nama, 
                (CASE 
                    WHEN left(klpk,1) != 'U' THEN (select kode from siakad.kode_fak where kode = left(klpk,3))
                    WHEN mk.nmmk like '%pancasila%' THEN (select kode from siakad.kode_fak where kode = 'A11')
                    WHEN mk.nmmk like '%kewarganegaraan%' THEN (select kode from siakad.kode_fak where kode = 'A12')
                    ELSE '' 
                END) as kode_prodi
                ,
                (CASE
                    WHEN left(klpk,1) != 'U' THEN (select jurusan from siakad.kode_fak where kode = left(klpk,3))
                    WHEN mk.nmmk like '%pancasila%' THEN (select jurusan from siakad.kode_fak where kode = 'A11')
                    WHEN mk.nmmk like '%kewarganegaraan%' THEN (select jurusan from siakad.kode_fak where kode = 'A12')
                    ELSE ''
                END) as nama_prodi
                ,jdwl.ta
                from siakad.krs_jadwal_archive jdwl
                join siakad.mk_reg_view mk 
                on jdwl.kdmk = mk.kdmk
            ) rs 
            where rs.kode_prodi = left(rs.kur_nama,3) and ta = {ta};
        '''
        self.Q_TA = '''
            select kode from siakad.tahun_ajaran
            where kode = %s
            and jns_smt != 0
            order by kode asc;
        '''
        self.Q_INSERT_DOSEN_ARCHIVE = '''  
            INSERT INTO {}
            SET nidn = %s, 
            nama_dosen = %s, kode_matkul = %s, nama_matkul = %s, jml_sks = %s,
            nama_kelas = %s, kode_prodi = %s, nama_prodi = %s,
            periode = %s;
        '''
        self.Q_GET_ID = '''
            SELECT * FROM {}
            WHERE nidn = %s and periode = %s and kode_matkul = %s and nama_kelas = %s
            limit 1;
        '''
        self.Q_GET_ID_WAREHOUSE_ALL = '''
            SELECT * FROM %s;
        '''
        self.Q_UPDATE_DOSEN_ARCHIVE = '''
            UPDATE {}
            SET nidn = %s, 
            nama_dosen = %s, kode_matkul = %s, nama_matkul = %s, jml_sks = %s,
            nama_kelas = %s, kode_prodi = %s, nama_prodi = %s,
            periode = %s
            WHERE nidn = %s and periode = %s and kode_matkul = %s and nama_kelas = %s;
        '''

    
    def get_data_dosen_perTa(self, active, tahun_ajaran, cursor):
        ta = tahun_ajaran
        cur = cursor
        #untuk fetch periode active
        if active:
            cur.execute(self.Q_GET_DOSEN_ACTIVE.format(ta=ta))
        else:
            cur.execute(self.Q_GET_DOSEN_ARCHIVE.format(ta=ta))
        res = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        dfDosen = pd.DataFrame(res, columns=columns)
        #Untuk mengecek jumlah data, jika tidak ada data, operasi dibatalkan
        if len(dfDosen) < 1:
            raise Exception("Hasil query kosong")
        return dfDosen

                    
    def insert_and_update_dosen_perTa(self, tahun_ajaran, cursor, filepath, conn):
        ta = tahun_ajaran
        cur = cursor

        dfDosen = pd.read_csv(filepath)
        dfDosen = dfDosen.astype(object).replace(np.nan, '')
        print(dfDosen.head())

        for index, row in dfDosen.iterrows():
            data = { 'nidn': row['nidn'], 
                    'nama_dosen': row['nama'], 'kode_matkul': row['kdmk'], 'nama_matkul': row['nmmk'], 
                    'jml_sks': row['sks'], 'nama_kelas': row['klpk'], 
                    'kode_prodi': row['kode_prodi'], 'nama_prodi': row['nama_prodi'],
                    'periode': row['ta']
                    }

            rows_count = cur.execute(self.Q_GET_ID.format(VAR_NAMA_TABEL_DOSEN_WH), (row['nidn'], row['ta'], row['kdmk'], row['klpk']))
            
            if rows_count > 0:
                cur.execute(self.Q_UPDATE_DOSEN_ARCHIVE.format(VAR_NAMA_TABEL_DOSEN_WH), (
                                        data['nidn'], data['nama_dosen'], 
                                        data['kode_matkul'], data['nama_matkul'], 
                                        data['jml_sks'], data['nama_kelas'], 
                                        data['kode_prodi'], data['nama_prodi'], 
                                        data['periode'],
                                        data['nidn'], data['periode'], data['kode_matkul'], data['nama_kelas']
                                        )
                        )
                conn.commit()
            else:
                cur.execute(self.Q_INSERT_DOSEN_ARCHIVE.format(VAR_NAMA_TABEL_DOSEN_WH), (
                                        data['nidn'], data['nama_dosen'], 
                                        data['kode_matkul'], data['nama_matkul'], 
                                        data['jml_sks'], data['nama_kelas'], 
                                        data['kode_prodi'], data['nama_prodi'], 
                                        data['periode']
                                        )
                        )
                conn.commit()
