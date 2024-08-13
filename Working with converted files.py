import echopype as ep
from dask.distributed import Client

# Membuat client dengan local scheduler
client = Client()

# Daftar jalur file .nc yang akan diproses
file_paths = [
    "./Users/abdammar/Desktop/Echopype Data/aa-D20231123-T234359.nc",
    "./Users/abdammar/Desktop/Echopype Data/aa-D20231124-T005648.nc"
]

# Membuat daftar objek EchoData dari setiap file .nc
ed_list = []
for file_path in file_paths:
    ed_list.append(ep.open_converted(file_path))  # Memuat file secara lazy-load

# Menggabungkan semua objek EchoData
combined_ed = ep.combine_echodata(
    ed_list, 
    client=client  # Menggunakan client Dask untuk pemrosesan paralel
)

# Menyimpan data gabungan ke dalam file NetCDF (.nc)
combined_ed_path = './Users/abdammar/Desktop/Echopype Data/combined_echodata.nc'
combined_ed.to_netcdf(combined_ed_path)
