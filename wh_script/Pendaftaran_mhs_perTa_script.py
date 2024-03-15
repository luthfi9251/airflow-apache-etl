import sys
import os  

from datetime import datetime
import base64
from pathlib import Path

import re
import pandas as pd
import numpy as np


class PendaftaranMhsPerTA:
    def __init__(self, is_dev=False):
        pass

    def get_data_mhs(self, tahun, cursor):
        cur = cursor
        sql = '''
            SELECT mhs.nim, mhs.nama, LEFT(mhs.`nim`,3) AS kode_prodi,
            (SELECT jurusan FROM siakad.`kode_fak` WHERE kode = LEFT(mhs.`nim`,3) ) AS nama_prodi,
            (SELECT jenjang FROM siakad.`kode_fak` WHERE kode = LEFT(mhs.`nim`,3) ) AS jenjang,
            '20212' AS periode, mhs.akdm_stat AS status_terakhir,
            pmb.`gel_daf` AS jenis_daftar,
            DATE(pmb.`date_reg_ulang`) AS tgl_daftar,
            (CASE
                WHEN mhs.akdm_stat = 3 THEN (SELECT DATE(klr.date_input) 
                                FROM siakad.`mahasiswa_keluar` klr 
                                WHERE klr.nim = mhs.nim 
                                ORDER BY klr.id DESC LIMIT 1)
                WHEN mhs.akdm_stat = 4 THEN (SELECT DATE(yud.tgl_yudisium) 
                                FROM siakad.`wisuda_calon_archive` yud 
                                WHERE yud.nim = mhs.nim 
                                ORDER BY yud.id DESC LIMIT 1)
                WHEN mhs.akdm_stat = 6 THEN (SELECT DATE(wft.tgl_surat) FROM siakad.`mahasiswa_meninggal` wft 
                                WHERE wft.nim = mhs.nim 
                                ORDER BY wft.id DESC LIMIT 1)
                WHEN mhs.akdm_stat = 7 THEN (SELECT dklr.tgl_surat FROM siakad.`mahasiswa_do` dklr 
                                WHERE dklr.nim = mhs.nim 
                                ORDER BY dklr.id DESC LIMIT 1)	
            END) AS tgl_keluar,
            (CASE
                WHEN mhs.akdm_stat = 3 THEN (SELECT (CASE 
                                    WHEN semester MOD 2 = 0 THEN CONCAT(tahun_ajar2,2)
                                    WHEN semester MOD 2 = 1 THEN CONCAT(tahun_ajar2,1)
                                    ELSE ''
                                END) AS periode
                                FROM siakad.`mahasiswa_keluar` klr 
                                WHERE klr.nim = mhs.nim 
                                ORDER BY klr.id DESC LIMIT 1)
                WHEN mhs.akdm_stat = 4 THEN (SELECT yud.ta
                                FROM siakad.`wisuda_calon_archive` yud 
                                WHERE yud.nim = mhs.nim 
                                ORDER BY yud.id DESC LIMIT 1)
                WHEN mhs.akdm_stat = 6 THEN (SELECT (CASE 
                                    WHEN semester MOD 2 = 0 THEN CONCAT(tahun_ajar2,2)
                                    WHEN semester MOD 2 = 1 THEN CONCAT(tahun_ajar2,1)
                                    ELSE ''
                                END) AS periode
                                FROM siakad.`mahasiswa_meninggal` wft 
                                WHERE wft.nim = mhs.nim 
                                ORDER BY wft.id DESC LIMIT 1)
                WHEN mhs.akdm_stat = 7 THEN (SELECT dklr.ta FROM siakad.`mahasiswa_do` dklr 
                                WHERE dklr.nim = mhs.nim 
                                ORDER BY dklr.id DESC LIMIT 1)	
            END) AS periode_keluar, mhs.`tgl_lahir`, 
            (SELECT nm_wil FROM forlap_dikti.`ews_wilayah`
            WHERE id_wil = mhs.`kecamatan`) AS kecamatan_asal
            FROM siadin.mhs mhs
            LEFT JOIN (SELECT nim, no_form, gel_daf, nisn, date_reg_ulang
                FROM siakad.pmb2009 
                UNION 
                SELECT nim, no_form, gel_daf, nisn, date_reg_ulang
                FROM siakad.pmb2009_history
            ) AS pmb
            ON mhs.nim = pmb.`nim`
            WHERE MID(mhs.nim,5,4) BETWEEN %s AND %s
            AND LEFT(mhs.nim,1) NOT IN ('Z')
            ORDER BY mhs.nim ASC;
        '''
        #membatasi 7 tahun kebelakang
        cur.execute(sql, (int(tahun) - 7, tahun))
        result = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        dfMhs = pd.DataFrame(result, columns=columns)

        return dfMhs
    
    def get_yudisium_data(self, cursor):
        cur = cursor
        sql_yud = '''
            SELECT nim, tgl_lahir, ipk, sks, bobot, ta, lama_studi, tgl_yudisium
            FROM siakad.wisuda_calon;
        '''
        cur.execute(sql_yud)
        result_yud = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        dfYud = pd.DataFrame(result_yud, columns=columns)

        sql_yud_archive = '''
                SELECT nim, wis.kode_yud, tgl_lahir, ipk, sks, bobot, ta, lama_studi, 
                -- tgl_yudisium, 
                ang.tgl_yud as tgl_yudisium 
                FROM siakad.wisuda_calon_archive wis
                JOIN siakad.angkatan_yudisium ang
                on wis.kode_yud = ang.kode_yud 
                WHERE MID(nim,5,4) >= 2010;
        '''
        cur.execute(sql_yud_archive)
        result_yud_archive = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        dfYud_archive = pd.DataFrame(result_yud_archive, columns=columns)

        return (dfYud, dfYud_archive)
    
    def transform_data(self, path):
        
        
        #diubah menjadi mengambil data dari excel
        # dfMhs2 = self.get_data_mhs(tahun)
        dfMhs2 = pd.read_csv(path.get("dfMhs"))
        dfYud = pd.read_csv(path.get("dfYud"))
        dfYud_archive = pd.read_csv(path.get("dfYud_archive"))

        dfYud["tgl_yudisium"] = pd.to_datetime(dfYud["tgl_yudisium"])
        dfYud_archive["tgl_yudisium"] = pd.to_datetime(dfYud_archive["tgl_yudisium"])

        #Mentransformasi data
        tgl_yud = []
        periode_yud = []
        ipk_lulus = []
        tgl_masuk = []
        
        total_data = len(dfMhs2)

        for index, row in dfMhs2.iterrows():
            ### Tanggal Mulai Kuliah ###
            th = row['nim'][4:8]
            date_str = '01-09-' + th
            dto = datetime.strptime(date_str, '%m-%d-%Y').date()
            tgl_masuk.append(dto.strftime("%Y-%m-%d"))
            ### End of Tanggal Mulai Kuliah ###
            

            ### Tanggal Yud ###
            if row['nim'] in dfYud.values :
                res = dfYud.loc[dfYud['nim'] == row['nim']].iloc[0]
                # print(res)
                if res['tgl_yudisium'] not in (None,0):            
                    tgl_yud.append(res['tgl_yudisium'].strftime("%Y-%m-%d"))
                else:
                    tgl_yud.append(None)
                periode_yud.append(res['ta'])
                ipk_lulus.append(res['ipk'])

            elif row['nim'] in dfYud_archive.values:
                res = dfYud_archive.loc[dfYud_archive['nim'] == row['nim']].iloc[0]
                # print(res)
                if res['tgl_yudisium'] not in (None,0):   
                    tgl_yud.append(res['tgl_yudisium'].strftime("%Y-%m-%d"))
                else:
                    tgl_yud.append(None)
                periode_yud.append(res['ta'])
                ipk_lulus.append(res['ipk'])
            else:
                tgl_yud.append('')
                ipk_lulus.append('')
                periode_yud.append('')

            ### End of Tanggal Yud ###
        dfMhs2["tgl_yudisium"] = tgl_yud
        dfMhs2["periode_yud"] = periode_yud 
        dfMhs2["ipk_lulus"] = ipk_lulus
        dfMhs2["tgl_masuk"] = tgl_masuk
        tahun_masuk = pd.DatetimeIndex(dfMhs2['tgl_masuk']).year
        dfMhs2["periode"] = tahun_masuk.astype(str) + "1"

        dfMhs2['kecamatan_asal'] = dfMhs2['kecamatan_asal'].str.replace('tidak ada','')

        
        #confirm pak Danny karena status terakhir ga ke rename, harus ubah ke str dulu
        dfMhs2["status_terakhir"] = dfMhs2["status_terakhir"].astype('str')

        ### Update tgl_keluar mhs lulus baca tabel yudisium ###
        dfMhs2.loc[dfMhs2['status_terakhir'] == '4', 'tgl_keluar'] = dfMhs2["tgl_yudisium"]
        dfMhs2.loc[dfMhs2['status_terakhir'] == '4', 'periode_keluar'] = dfMhs2["periode_yud"]
        
        ### Update akdm_stat ###
        dfMhs2.loc[dfMhs2.status_terakhir == '1','status_terakhir'] = 'Aktif'
        dfMhs2.loc[dfMhs2.status_terakhir == '2','status_terakhir'] = 'Cuti'
        dfMhs2.loc[dfMhs2.status_terakhir == '3','status_terakhir'] = 'Keluar'
        dfMhs2.loc[dfMhs2.status_terakhir == '4','status_terakhir'] = 'Lulus'
        dfMhs2.loc[dfMhs2.status_terakhir == '5','status_terakhir'] = 'Tidak Aktif'
        dfMhs2.loc[dfMhs2.status_terakhir == '6','status_terakhir'] = 'Meninggal'
        dfMhs2.loc[dfMhs2.status_terakhir == '7','status_terakhir'] = 'DO'
        # dfListUpdate2.loc[(dfListUpdate2.status_terakhir == '8'),'status_terakhir']='Aktif Keuangan'

        ### Hitung Masa Studi ###
        dfMhs2['tgl_keluar'].astype(str)
        
        if dfMhs2['tgl_keluar'] is not None | dfMhs2['tgl_keluar'] is not '':
            keluar = pd.to_datetime(dfMhs2['tgl_keluar'], errors='coerce')    
            ms_stud = (pd.DatetimeIndex(keluar).year - pd.DatetimeIndex(dfMhs2['tgl_masuk']).year) * 12 + (pd.DatetimeIndex(keluar).month - pd.DatetimeIndex(dfMhs2['tgl_masuk']).month)
        else:
            ms_stud = None
        dfMhs2 = dfMhs2.assign(masa_studi=ms_stud)

        dfMhs2['masa_studi'] = dfMhs2['masa_studi'].astype(str)


        dfMhs2 = dfMhs2.astype(object).replace(np.nan, '')
        dfMhs2 = dfMhs2.replace({np.nan: None})
        return dfMhs2
    
    def get_data_wh_exist(self, cursor):
        cur = cursor
        ### GET DATA wh_mahasiswa_pendaftaran akdm_stat not in (3,4,6,7) ###
        q_wh_list_exclude_update = '''  
            select * from warehouse.wh_mahasiswa_pendaftaran
            where status_terakhir in ('Keluar', 'Lulus', 'DO', 'Meninggal');
        '''


        cur.execute(q_wh_list_exclude_update)
        result = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        dfMhsWh = pd.DataFrame(result, columns=columns)
        return dfMhsWh
    
    def compare_data(self, path):
        
        dfMhs = pd.read_csv(path.get("dfMhs"))
        dfMhsWh = pd.read_csv(path.get("dfMhsWh"))
        
        dfListUpdate = dfMhs[~dfMhs.nim.isin(dfMhsWh.nim)]   
        return dfListUpdate
    
    def load_data(self, path, cursor, conn):
        cur = cursor
        dfFiltered = pd.read_csv(path.get("dfFiltered"))

        #confirm pak danny buat replace nan dengan string kosong, karena nan gabisa dipake query mysql
        dfFiltered = dfFiltered.astype(object).replace(np.nan, '')        

        ### Query Insert tabel mahasiswa_pendaftaran ###

        q_insert = '''  
            INSERT INTO warehouse.wh_mahasiswa_pendaftaran
            SET nim = %s, nama = %s, kode_prodi = %s, nama_prodi = %s,
            jenjang = %s, periode = %s, status_terakhir = %s, jenis_daftar = %s,
            tgl_daftar = %s, tgl_keluar = %s, periode_keluar = %s, 
            tgl_lahir = %s,
            asal_kecamatan = %s, ipk_lulus = %s, masa_studi = %s;
        '''

        q_get_id = '''
            SELECT * FROM warehouse.wh_mahasiswa_pendaftaran
            WHERE nim = %s and periode = %s
            -- AND status_terakhir not in (3,4,6,7) limit 1;
            limit 1;
        '''

        q_get_id_warehouse_all = '''
            SELECT * FROM warehouse.wh_mahasiswa_pendaftaran;
        '''

        q_update = '''
            UPDATE warehouse.wh_mahasiswa_pendaftaran
            SET nim = %s, nama = %s, kode_prodi = %s, nama_prodi = %s,
            jenjang = %s, periode = %s, status_terakhir = %s, jenis_daftar = %s,
            tgl_daftar = %s, tgl_keluar = %s, periode_keluar = %s, 
            tgl_lahir = %s,
            asal_kecamatan = %s, ipk_lulus = %s, masa_studi = %s
            WHERE nim = %s and periode = %s;
        '''
        
        total_data = len(dfFiltered)

        for index, dataframe in enumerate(dfFiltered.iterrows()):
            row = dataframe[1]

            data = { 'nim': row['nim'], 'nama': row['nama'], 'kode_prodi': row['kode_prodi'], 
                    'nama_prodi': row['nama_prodi'],
                    'jenjang': row['jenjang'], 'periode': row['periode'], 'status_terakhir': row['status_terakhir'], 
                    'jenis_daftar': row['jenis_daftar'],
                    'tgl_daftar': row['tgl_masuk'], 'tgl_keluar': row['tgl_keluar'], 
                    'periode_keluar': row['periode_keluar'], 'tgl_lahir': row['tgl_lahir'],
                    'asal_kecamatan': row['kecamatan_asal'], 'ipk_lulus': row['ipk_lulus'], 
                    'masa_studi': row['masa_studi']              
                    }

            ### get kurikulum item id SiAkad ###
            rows_count = cur.execute(q_get_id, (row['nim'], row['periode']))
            res3 = cur.fetchall()

            if rows_count > 0:
                cur.execute(q_update, (
                                        data['nim'], data['nama'], data['kode_prodi'], 
                                        data['nama_prodi'],
                                        data['jenjang'], data['periode'], data['status_terakhir'], 
                                        data['jenis_daftar'],
                                        data['tgl_daftar'], data['tgl_keluar'], 
                                        data['periode_keluar'], data['tgl_lahir'],
                                        data['asal_kecamatan'], data['ipk_lulus'], 
                                        data['masa_studi'],
                                        data['nim'], data['periode']
                                        )
                        )
                conn.commit()

            else:      
                cur.execute(q_insert, (
                                        data['nim'], data['nama'], data['kode_prodi'], 
                                        data['nama_prodi'],
                                        data['jenjang'], data['periode'], data['status_terakhir'], 
                                        data['jenis_daftar'],
                                        data['tgl_daftar'], data['tgl_keluar'], 
                                        data['periode_keluar'], data['tgl_lahir'],
                                        data['asal_kecamatan'], data['ipk_lulus'], 
                                        data['masa_studi']
                                        )
                        )
                conn.commit()

    def run(self, tahun, tanggal):
        
        # self.get_data_mhs(tahun)
        # self.transform_data()
        # self.get_data_wh_exist()

        self.load_data()


    
# tahun = 2023 # Untuk Limit Angkatan Mahasiswa query: BETWEEN 2015 and tahun
# tgl = 241123 # Tanggal hari ini ntuk penamaan file excel yang disimpan

# # hsl = mhs.compare_data(tahun, tgl)
# # hsl

# PendaftaranMhsPerTA().run(tahun, tgl)