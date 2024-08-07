## การรันโปรแกรม ตอน dev

### Build Docker Image
`docker build -t my-jupyter . `

### Run Docker Container
`docker run -p 8888:8888 my-jupyter`

## Access Jupyter Notebook / select kernel

Open your web browser and go to [http://localhost:8888]

just copy paste token and click on login.

### ref
https://medium.com/@18bhavyasharma/setting-up-and-running-jupyter-notebook-in-a-docker-container-d2acd713ce66


### เรียกใช้คำสั่ง Docker

`docker-compose up -d -- build`

### ตรวจสอบว่า container ถูกสร้างสำเร็จ
`docker ps`

### เข้าใช้งาน phpMyAdmin
`http://localhost:8080`

เข้าใช้งาน r506-db ให้ระบุดังนี้

` Server: r506-db
Username: user
Password: user
`
### หรือ เช้าใช้ผ่าน NAVICAT

` 
Host: localhost
PORT: 6001
Username: user
Password: user
`
