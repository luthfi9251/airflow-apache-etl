import base64
from pathlib import Path
import os

import re
import pandas as pd
import numpy as np

#custom depencency
from wh_script.utils.tahunajaran import TahunAjaran
from wh_script.utils.progress_info import use_progress_tracker

class KuliahMhsPerTa:
    def __init__(self):
        self.Q_GET_DATA_MHS = '''
            SELECT mhs.nim, mhs.akdm_stat,
            LEFT(mhs.nim,3) AS kode_prodi,
            kdfak.jurusan AS nama_jurusan,
            kdfak.`jenjang` AS jenjang,
            mhs.nama
            FROM siadin.`mhs` AS mhs
            JOIN siakad.`kode_fak` AS kdfak
            ON kdfak.`kode` = LEFT(mhs.nim,3)
            -- AND (MID(mhs.nim,5,4) BETWEEN 2015 AND 2021)
            AND (MID(mhs.nim,5,4) BETWEEN {thn_after} AND {thn_before})
            -- AND mhs.nim like '%.2022.%'
            AND LEFT(mhs.nim,1) != 'Z'
            ORDER BY nim;
        '''
        self.Q_GET_TA = '''
            SELECT kode 
            FROM siakad.`tahun_ajaran` 
            WHERE kode = {ta}
            -- jns_smt IN (1,2) AND tahun_awal BETWEEN 2015 AND 2021
            ORDER BY tahun_awal ASC;
        '''
        self.Q_GET_IPS = '''
            SELECT * FROM siakad.`ips` where ta = {ta};
        '''
        self.Q_GET_PENDAFTAR_WH = '''
            SELECT * FROM warehouse.`wh_mahasiswa_pendaftaran`;
        '''
        self.Q_GET_DATA_MHS_WH = '''
            select * 
            from warehouse.wh_mahasiswa_kuliah 
            where periode = {ta};
        '''
        self.Q_GET_IPK_WH = '''
            select * 
            -- from siakad.ipk
            from warehouse.wh_ipk 
            where periode = {ta};
        '''
        self.Q_INSERT_KULIAH_WH = '''  
            INSERT INTO warehouse.wh_mahasiswa_kuliah
            SET nim = %s, nama = %s, kode_prodi = %s, nama_prodi = %s,
            jenjang = %s, periode = %s, status_akademik = %s, ips = %s,
            ipk = %s, sks_sem = %s
                
        '''
        self.Q_GET_ID_KULIAH_PERIODE_WH = '''
            SELECT * FROM warehouse.wh_mahasiswa_kuliah
            WHERE nim = %s and periode = %s
            -- AND status_terakhir not in (3,4,6,7) limit 1;
            limit 1;
        '''
        self.Q_GET_ID_KULIAH_WH_ALL = '''
            SELECT * FROM warehouse.wh_mahasiswa_kuliah;
        '''
        self.Q_UPDATE_KULIAH_WH = '''
            UPDATE warehouse.wh_mahasiswa_kuliah
            SET nim = %s, nama = %s, kode_prodi = %s, nama_prodi = %s,
            jenjang = %s, periode = %s, status_akademik = %s, ips = %s,
            ipk = %s, sks_sem = %s 
            WHERE nim = %s and periode = %s;
        '''

    def get_data_mhs_periode(self, ta, cursor):
        cur = cursor
        cur.execute(self.Q_GET_DATA_MHS.format(thn_after=int(ta[0:4])-6, thn_before=ta[0:4]))
        result = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        dfMhs = pd.DataFrame(result, columns=columns)

        #Mendapatkan kode TA
        cur.execute(self.Q_GET_TA.format(ta=ta))
        result2 = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        dfTa = pd.DataFrame(result2, columns=columns)

        lst = []
        new_idx = -1
        total_data= len(dfMhs)
        for index, row in dfMhs.iterrows():
            for index2, ta in dfTa.iterrows():
                data = {'nim': row['nim'], 'akdm_stat': row['akdm_stat'], 'kode_prodi': row['kode_prodi'],
                        'nama_jurusan': row['nama_jurusan'], 'jenjang': row['jenjang'],
                        'nama': row['nama'],
                        'periode': ta['kode']
                    }
                new_idx += 1
                # print(new_idx, " - ",data)
                lst.append(data)

        #Export dataframe ke excel
        dfMerge = pd.DataFrame(data=lst)
        return dfMerge

    def get_data_ips_mhs(self, ta, cursor):
        cur = cursor
        cur.execute(self.Q_GET_IPS.format(ta=ta))
        result3 = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        dfIps = pd.DataFrame(result3, columns=columns)

        if len(dfIps) < 1:
            raise Exception("Hasil query kosong")
        
        return dfIps

    def get_data_pendaftar_wh(self, cursor):
        cur = cursor
        cur.execute(self.Q_GET_PENDAFTAR_WH)
        result4 = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        dfPendaftar = pd.DataFrame(result4, columns=columns)

        return dfPendaftar

    def get_data_mhs_wh(self, cursor, ta):
        cur = cursor

        ta_sebelumnya = TahunAjaran.get_periode_sebelum(ta)
        cur.execute(self.Q_GET_DATA_MHS_WH.format(ta=ta_sebelumnya))
        result5 = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df_wh_kuliah = pd.DataFrame(result5, columns = columns)

        return df_wh_kuliah
    
    def get_data_ipk_wh(self, cursor, ta):
        cur = cursor

        cur.execute(self.Q_GET_IPK_WH.format(ta=ta))
        result6 = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df_wh_ipk = pd.DataFrame(result6, columns=columns)

        return df_wh_ipk


    def add_ips_mhs_periode(self, ta, path):


        dfKuliah = pd.read_csv(path.get("dfKuliah"))

        #Mengubah kode menjadi semantic
        dfKuliah.loc[(dfKuliah.akdm_stat == 1),'akdm_stat']='Aktif'
        dfKuliah.loc[(dfKuliah.akdm_stat == 2),'akdm_stat']='Cuti'
        dfKuliah.loc[(dfKuliah.akdm_stat == 3),'akdm_stat']='Keluar'
        dfKuliah.loc[(dfKuliah.akdm_stat == 4),'akdm_stat']='Lulus'
        dfKuliah.loc[(dfKuliah.akdm_stat == 5),'akdm_stat']='Tidak Aktif'
        dfKuliah.loc[(dfKuliah.akdm_stat == 6),'akdm_stat']='Meninggal'
        dfKuliah.loc[(dfKuliah.akdm_stat == 7),'akdm_stat']='DO'
        dfKuliah.loc[(dfKuliah.akdm_stat == 8),'akdm_stat']='Aktif'

        dfIps = pd.read_csv(path.get("dfIps"))
            #handle ketika tidak ada data ditemukan
        

        #menggabungkan data kuliah mhs dengan data ips
        ips = []
        sks_sem = []

        total_data = len(dfKuliah)
        i = 0
        (step, part, thresshold) = use_progress_tracker(total_data)
        print(f"Processing {total_data}")

        for index, row in dfKuliah.iterrows():
            i+=1
            if i >= thresshold:
                print(f"{i} from {total_data} : {i/total_data * 100}%")
                step+=1
                thresshold = part * step
            
            res = dfIps.loc[(dfIps['nim'] == row['nim']) & (dfIps['ta'] == int(row['periode']))]
            if res.empty:
                ips.append(0)
                sks_sem.append(0)
            else:
                ips.append(res['ips'].values[0])
                sks_sem.append(res['sks'].values[0])

        dfKuliah['ips'] = ips
        dfKuliah['sks_sem'] = sks_sem
        return dfKuliah
        #END PROCESS add_ips_mhs_periode

    def filter_angkatan_mhs(self, path):
        dfKuliahIps = pd.read_csv(path.get("dfKuliahIps"))

        #filter angkatan lebih dari tahun periode
        dfKuliahIps['angkatan'] = dfKuliahIps['nim'].astype(str).str[4:8]
        dfKuliahIps['th_periode'] = dfKuliahIps['periode'].astype(str).str[:4]
        dfKuliahIps['gg_periode'] = dfKuliahIps['periode'].astype(str).str[-1:]
                    # Filter tahun periode yang dibawah angkatan mhs
        dfFiltered = dfKuliahIps.loc[(dfKuliahIps['angkatan'] <= dfKuliahIps['th_periode'])]

        dfPendaftar = pd.read_csv(path.get("dfPendaftar"))

        periode_keluar = []
        total_data = len(dfFiltered)
        i = 0
        (step, part, thresshold) = use_progress_tracker(total_data)
        print(f"Processing {total_data}")
        for index, row in dfFiltered.iterrows():
            i+=1
            if i >= thresshold:
                print(f"{i} from {total_data} : {i/total_data * 100}%")
                step+=1
                thresshold = part * step

            res = dfPendaftar.loc[(dfPendaftar['nim'] == row['nim'])]
            if res.empty:
                periode_keluar.append('')
            else:
                periode_keluar.append(res['periode_keluar'].values[0])
        dfFiltered['periode_keluar'] = periode_keluar

        #memfilter data dengan perode kosong atau periode dibawah periode keluar
        dfFiltered3 = dfFiltered.loc[
                                (dfFiltered['periode'].astype(str) <= dfFiltered['periode_keluar'].astype(str)) | 
                                (dfFiltered['periode_keluar'] == '')
                            ]
        return dfFiltered3
        #END Process filter_angkatan_mhs

    def add_ipk_semester(self, path, ta):
        dfKuliahIpsFiltered = pd.read_csv(path.get("dfKuliahIpsFiltered"))

        df_wh_kuliah = pd.read_csv(path.get("df_wh_kuliah"))
        
        #Mendapatkan ipk dari tabel WH periode saat ini
        df_wh_ipk = pd.read_csv(path.get("df_wh_ipk"))

        #Menambahkan kolom semester dan IPK
        semester = []
        ipk = []
        total_data = len(dfKuliahIpsFiltered)
        i = 0
        (step, part, thresshold) = use_progress_tracker(total_data)
        print(f"Processing {total_data}")

        for index, row in dfKuliahIpsFiltered.iterrows():
            i+=1
            if i >= thresshold:
                print(f"{i} from {total_data} : {i/total_data * 100}%")
                step+=1
                thresshold = part * step

            res = df_wh_kuliah.loc[(df_wh_kuliah['nim'] == row['nim'])]
            res2 = df_wh_ipk.loc[(df_wh_ipk['nim'] == row['nim']) & (df_wh_ipk['periode'] == int(ta))]

            if res.empty:
                semester.append('')
                # ipk.append('')
            else:
                semester.append(int(res['semester'].values[0]) + 1)
                
            if res2.empty:
                ipk.append('')
            else:
                ipk.append(res2['ipk'].values[0])
                
        dfKuliahIpsFiltered['semester'] = semester
        dfKuliahIpsFiltered['ipk'] = ipk
        return dfKuliahIpsFiltered

    def prepare_data_mhs(self):
        self.get_data_mhs_periode()
        self.add_ips_mhs_periode()
        self.filter_angkatan_mhs()
        self.add_ipk_semester()
    
    def syncronize_to_db(self, path, cursor, conn):
        cur = cursor

        #Membuka file berisi data yang sudah jadi dan ubah ips kosong menjadi ''
        dfKuliahIpsFiltered = pd.read_csv(path)
        hsl = dfKuliahIpsFiltered.loc[(dfKuliahIpsFiltered['ips'] != 0)].replace(np.nan, '')

        
        #Memulai proses update dan insert ke database
        total_data = len(hsl)
        i = 0
        (step, part, thresshold) = use_progress_tracker(total_data)
        print(f"Processing {total_data}")
        for index, item in enumerate(hsl.iterrows()):
            i+=1
            if i >= thresshold:
                print(f"{i} from {total_data} : {i/total_data * 100}%")
                step+=1
                thresshold = part * step

            row = item[1]

            data = { 'nim': row['nim'], 'nama': row['nama'], 'kode_prodi': row['kode_prodi'], 
                    'nama_prodi': row['nama_jurusan'],
                    'jenjang': row['jenjang'], 'periode': row['periode'], 'status_akademik': row['akdm_stat'], 
                    'ips': row['ips'],'ipk': row['ipk'], 'sks_sem': row['sks_sem']
                    }
            ### get id warehouse kuliah mhs ###
            rows_count = cur.execute(self.Q_GET_ID_KULIAH_PERIODE_WH, (row['nim'], row['periode']))
            res3 = cur.fetchall()
            if rows_count > 0:
                cur.execute(self.Q_UPDATE_KULIAH_WH, (
                                        data['nim'], data['nama'], data['kode_prodi'], 
                                        data['nama_prodi'],
                                        data['jenjang'], data['periode'], data['status_akademik'], 
                                        data['ips'], data['ipk'], data['sks_sem'],
                                        data['nim'], data['periode']
                                        )
                        )
                conn.commit()

            else:
                cur.execute(self.Q_INSERT_KULIAH_WH, (
                                        data['nim'], data['nama'], data['kode_prodi'], 
                                        data['nama_prodi'],
                                        data['jenjang'], data['periode'], data['status_akademik'], 
                                        data['ips'], data['ipk'], data['sks_sem']
                                        )
                        )
                conn.commit()

    def run(self, ta):
        return



# KuliahMhsPerTa().run(20221)