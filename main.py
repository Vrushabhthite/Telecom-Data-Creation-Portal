
import os

from fastapi import FastAPI, UploadFile, File, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from save_file import save_file
from database import engine, Base
import models

Base.metadata.create_all(bind=engine)


from logic import(
    access_ciena_logic,
    access_eci_logic,
    access_huawei_logic,
    access_tejas_logic,
    dwdm_ciena_logic,
    dwdm_huawei_logic,
    dwdm_nokia_logic,
    dwdm_zte_logic
)

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
from fastapi.staticfiles import StaticFiles

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# common circles
circles = ["APR","ANE","BIH","DEL","GUJ","HAR","HPR","JNK","KAR","KER","MAH","MPC","MUM","ODI","PJB","RAJ","TNC","UPE","UPW","WBL"]

# ================= ROUTES =================

# ================= AUTH CHECK =================

from fastapi.responses import RedirectResponse

def check_auth(request: Request):
    user = request.cookies.get("user")

    if not user:
        return RedirectResponse(url="/?error=Please login first", status_code=302)

    return user



# ================= ROUTES =================

# LOGIN PAGE (ROOT)
@app.get("/", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


#  REGISTER PAGE
@app.get("/register_page", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    user = check_auth(request)

    if isinstance(user, RedirectResponse):
        return user

    response = templates.TemplateResponse("dashboard.html", {
        "request": request,
        "circles": circles,
        "user": user
    })

   
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response
# POC (PROTECTED)
@app.get("/poc", response_class=HTMLResponse)
async def poc_page(request: Request):
    user = check_auth(request)

    if isinstance(user, RedirectResponse):
        return user

    response= templates.TemplateResponse("poc.html", {
        "request": request,
        "circles": circles,
        "user": user
    })
    
    
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response


# MAIN1 (PROTECTED)
@app.get("/main1", response_class=HTMLResponse)
async def main1_page(request: Request):
    user = check_auth(request)

    if isinstance(user, RedirectResponse):
        return user

    response= templates.TemplateResponse("main1.html", {
        "request": request,
        "circles": circles,
        "user": user
    })
    
    
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response


# MAIN2 (PROTECTED)
@app.get("/main2", response_class=HTMLResponse)
async def main2_page(request: Request):
    user = check_auth(request)

    if isinstance(user, RedirectResponse):
        return user

    response= templates.TemplateResponse("main2.html", {
        "request": request,
        "circles": circles,
        "user": user
    })
    
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response


# TRANSFORM PAGES (ALL PROTECTED)

@app.get("/Exclation_Matrix", response_class=HTMLResponse)
async def exclation_matrix(request: Request):
    user = check_auth(request)
    if isinstance(user, RedirectResponse):
        return user

    response= templates.TemplateResponse("exclation_matrix.html", {
        "request": request,
        "circles": circles,
        "user": user
    })
    
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response


@app.get("/Super_BSC", response_class=HTMLResponse)
async def super_bsc(request: Request):
    user = check_auth(request)
    if isinstance(user, RedirectResponse):
        return user

    response= templates.TemplateResponse("super_bsc.html", {
        "request": request,
        "circles": circles,
        "user": user
    })
    
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response


@app.get("/Super_Router", response_class=HTMLResponse)
async def super_router(request: Request):
    user = check_auth(request)
    if isinstance(user, RedirectResponse):
        return user

    response= templates.TemplateResponse("super_router.html", {
        "request": request,
        "circles": circles,
        "user": user
    })
    
    
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response


@app.get("/Super_wan", response_class=HTMLResponse)
async def super_wan(request: Request):
    user = check_auth(request)
    if isinstance(user, RedirectResponse):
        return user

    response= templates.TemplateResponse("super_wan.html", {
        "request": request,
        "circles": circles,
        "user": user
    })
    
    
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response


@app.get("/Voxid", response_class=HTMLResponse)
async def vooxid_data(request: Request):
    user = check_auth(request)
    if isinstance(user, RedirectResponse):
        return user

    response= templates.TemplateResponse("vooxid.html", {
        "request": request,
        "circles": circles,
        "user": user
    })
    
    
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response






#==================Auth System===============

from fastapi.responses import RedirectResponse

def check_auth(request: Request):
    user = request.cookies.get("user")

    if not user:
        return RedirectResponse(url="/?error=Please login first", status_code=302)

    return user


#================Registr API===============

from database import SessionLocal
from models import User
from logic.auth import hash_password
import re
@app.post("/register")
async def register(
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...)
):
    db = SessionLocal()

    username = username.strip()
    email = email.strip()

    # ✅ Check username exists
    existing_user = db.query(User).filter(User.username == username).first()
    if existing_user:
        db.close()
        return RedirectResponse(
            url="/register_page?error=Username already exists",
            status_code=302
        )

    # Check email exists
    existing_email = db.query(User).filter(User.email == email).first()
    if existing_email:
        db.close()
        return RedirectResponse(
            url="/register_page?error=Email already registered",
            status_code=302
        )

    #Create user
    new_user = User(
        username=username,
        email=email,
        password=hash_password(password.strip())
    )

    db.add(new_user)
    db.commit()
    db.close()

    return RedirectResponse(
        url="/?msg=Registration successful",
        status_code=302
    )

#=================Login API=================

from logic.auth import verify_password
@app.post("/login")
async def login(email: str = Form(...), password: str = Form(...)):
    db = SessionLocal()

    user = db.query(User).filter(User.email == email.strip()).first()
    db.close()

    if not user:
        return RedirectResponse(url="/?error=User not found", status_code=302)

    if not verify_password(password.strip(), user.password):
        return RedirectResponse(url="/?error=Wrong password", status_code=302)

    response = RedirectResponse(url="/dashboard", status_code=302)

    response.set_cookie(
        key="user",
        value=user.email,
        httponly=True,
        path="/",
        samesite="lax"
    )

    return response


#================Logout  API=================

@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/?msg=Logout successful", status_code=302)

    response.delete_cookie("user")

    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response

# ================= GENERATE =================

@app.post("/generate", response_class=HTMLResponse)
async def generate_output(request: Request, circle: str = Form(...), tech_type: str = Form(...),
                          oem: str = Form(...), 
                          service_file: UploadFile = File(...),
                          tunnel_file: UploadFile = File(...),
                          single_file: UploadFile = File(...),
                          ):
    try:
        tech_type = tech_type.lower()
        oem = oem.lower()

        service_path = save_file(service_file)
        tunnel_path = save_file(tunnel_file)
        single_path = save_file(single_file)
       

        output_path = None

        if tech_type == "access" and oem == "ciena":
            output_path = access_ciena_logic.run(service_path, tunnel_path, circle)

        elif tech_type == "access" and oem == "eci":
            output_path = access_eci_logic.run(service_path, tunnel_path, circle)

        elif tech_type == 'access' and oem == 'huawei':
            output_path = access_huawei_logic.run(service_path, tunnel_path, circle)

        elif tech_type == 'access' and oem == 'tejas':
            output_path = access_tejas_logic.run(service_path, tunnel_path, circle)

        elif tech_type == 'dwdm' and oem == 'nokia':
            output_path = dwdm_nokia_logic.run(service_path, tunnel_path, circle)
            
        elif tech_type == 'dwdm' and oem == 'ciena':
            output_path = dwdm_ciena_logic.run(service_path, circle)
        
        elif tech_type == 'dwdm' and oem == 'zte':
            output_path = dwdm_zte_logic.run(service_path, circle)

        elif tech_type == 'dwdm' and oem == 'huawei':
            output_path = dwdm_huawei_logic.run(service_path, tunnel_path, single_path ,circle)

        if output_path is None:
            raise Exception("Invalid tech_type or oem")

        return FileResponse(output_path,
                            filename=f"{tech_type}_{circle}_{oem}_output.xlsx",
                            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    except Exception as e:
        return templates.TemplateResponse("dashboard.html", {"request": request, "error": str(e), "circles": circles})



# ================= POC =================

from logic.poc_logic import run_poc

@app.post("/poc_generate", response_class=HTMLResponse)
async def poc_generate(request: Request, circle: str = Form(...),
                       ran_count_file: UploadFile = File(...),
                       scope_inventory_file: UploadFile = File(...),
                       mw_tree_file: UploadFile = File(...),
                       mtx_file: UploadFile = File(...),
                       site_info_file: UploadFile = File(...)):
    try:
        p1 = save_file(ran_count_file)
        p2 = save_file(scope_inventory_file)
        p3 = save_file(mw_tree_file)
        p4 = save_file(mtx_file)
        p5 = save_file(site_info_file)

        output_path = run_poc(circle, p1, p2, p3, p4, p5)

        return FileResponse(output_path)

    except Exception as e:
        return templates.TemplateResponse("poc.html", {"request": request, "error": str(e), "circles": circles})




# ================= MAIN1 =================

from logic.main_logic.main1 import process_main1

@app.post("/generate_main1", response_class=HTMLResponse)
async def generate_main1(request: Request, circle: str = Form(...),
                         eci_file: UploadFile = File(None),
                         huawei_file: UploadFile = File(None),
                         ciena_file: UploadFile = File(None),
                         tejas_file: UploadFile = File(None),
                         dwdm_huawei_file: UploadFile = File(None),
                         dwdm_ciena_file: UploadFile = File(None),
                         dwdm_zte_file: UploadFile = File(None),
                         dwdm_nokia_file: UploadFile = File(None),
                         wan_file: UploadFile = File(...)):
    try:
        output_path = process_main1(
            save_file(eci_file) if eci_file else None,
            save_file(huawei_file) if huawei_file else None,
            save_file(ciena_file) if ciena_file else None,
            save_file(tejas_file) if tejas_file else None,
            save_file(dwdm_huawei_file) if dwdm_huawei_file else None,
            save_file(dwdm_ciena_file) if dwdm_ciena_file else None,
            save_file(dwdm_zte_file) if dwdm_zte_file else None,
            save_file(dwdm_nokia_file) if dwdm_nokia_file else None,
            save_file(wan_file),
            circle
        )
        return FileResponse(output_path)

    except Exception as e:
        return templates.TemplateResponse("main1.html", {"request": request, "error": str(e), "circles": circles})

# ================= MAIN2 =================

from logic.main_logic.main2 import process_main2

@app.post("/generate_main2", response_class=HTMLResponse)
async def generate_main2(request: Request, circle: str = Form(...),
                         main1_file: UploadFile = File(...),
                         poc_file: UploadFile = File(None),
                         poc_router_file: UploadFile = File(None),
                         bsc_file: UploadFile = File(None)):
    try:
        output_path = process_main2(
            save_file(main1_file),
            save_file(poc_file) if poc_file else None,
            save_file(poc_router_file) if poc_router_file else None,
            save_file(bsc_file) if bsc_file else None,
            circle
        )
        return FileResponse(output_path, filename=os.path.basename(output_path), media_type="text/csv")

    except Exception as e:
        return templates.TemplateResponse("main2.html", {"request": request, "error": str(e), "circles": circles})



# ================= TRANSFORM =================

from logic.transform_logic import run_transform

@app.post("/transform", response_class=HTMLResponse)
async def transform_output(request: Request, circle: str = Form(...),
                           transform_type: str = Form(...),
                           input_file: UploadFile = File(...)):
    try:
        output_path = run_transform(save_file(input_file), circle, transform_type)

        return FileResponse(output_path,
                            filename=os.path.basename(output_path),
                            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    except Exception as e:
        return templates.TemplateResponse("dashboard.html", {"request": request, "error": str(e), "circles": circles})


































# @app.post("/generate")
# async def generate_output(
#     circle: str = Form(...),
#     tech_type: str = Form(...),
#     oem: str = Form(...),
#     service_file: UploadFile = File(...),
#     tunnel_file: UploadFile = File(...)
# ):

#     tech_type = tech_type.lower()
#     oem = oem.lower()

#     service_path = save_file(service_file)
#     tunnel_path = save_file(tunnel_file)

#     output_path = None

#     if tech_type == "access" and oem == "ciena":

#         output_path = access_ciena_logic.run(
#             service_path,
#             tunnel_path,
#             circle
#         )
        
#     elif tech_type == "access" and oem == "eci":

#         output_path = access_eci_logic.run(
#             service_path,
#             tunnel_path,
#             circle
#         )
        
#     elif tech_type=='access' and oem=='huawei':
#         output_path=access_huawei_logic.run(
#             service_path,
#             tunnel_path,
#             circle
#         )
    
#     elif tech_type=='access' and oem=='tejas':
#         output_path=access_tejas_logic.run(
#             service_path,
#             tunnel_path,
#             circle
#         )
        
        
#     elif tech_type=='dwdm' and oem=='nokia':
#         output_path=dwdm_nokia_logic.run(
#             service_path,
#            tunnel_path,
#            circle)
        
#     print("TECH:", tech_type)
#     print("OEM:", oem)
   
    
    
#     if output_path is None:
#      return {"error": "Invalid tech_type or oem"}

#     # Save file info to database
#     # from database import SessionLocal
#     # from models import FileLog
#     # import os

#     # db = SessionLocal()

#     # log = FileLog(
#     #     circle=circle,
#     #     oem=f"{tech_type.upper()} - {oem.upper()}",
#     #     output_file=output_path
#     # )

#     # db.add(log)
#     # db.commit()
#     # db.close()

#     response = FileResponse(
#         output_path,
#         filename=f"{tech_type}_{circle}_{oem}_output.xlsx",
#         media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#     )

#     response.headers["X-Status"] = "success"
#     response.headers["X-Message"] = "File generated successfully"

#     return response


# #Poc data

# from logic.poc_logic import run_poc

# @app.post("/poc_generate")
# async def poc_generate(
#     circle: str = Form(...),
#     ran_count_file: UploadFile = File(...),
#     scope_inventory_file: UploadFile = File(...),
#     mw_tree_file: UploadFile = File(...),
#     mtx_file: UploadFile = File(...),
#     site_info_file: UploadFile = File(...)
# ):

#     path1 = save_file(ran_count_file)
#     path2 = save_file(scope_inventory_file)
#     path3 = save_file(mw_tree_file)
#     path4 = save_file(mtx_file)
#     path5 = save_file(site_info_file)

#     output_path = run_poc(circle, path1, path2, path3, path4, path5)

#     return FileResponse(
#         output_path,
#         filename=os.path.basename(output_path),
#         media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#     )
    
    
# # WanDB data

# # from logic.wandb import process_wan_db

# # @app.post("/wandb_generate")
# # async def wandb_generate(
# #     Circle: str = Form(...),
# #     wan_db_file: UploadFile = File(...)
# # ):
# #     file_path = save_file(wan_db_file)

# #     output_path = process_wan_db(file_path,Circle)

# #     return FileResponse(
# #         output_path,
# #         filename=os.path.basename(output_path),
# #         media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
# #     )

    
# #main1

# from logic.main_logic.main1 import process_main1
# @app.post("/generate_main1")
# async def generate_main1(
#     circle: str = Form(...),

#     eci_file: UploadFile = File(None),
#     huawei_file: UploadFile = File(None),
#     ciena_file: UploadFile = File(None),
#     tejas_file: UploadFile = File(None),

#     dwdm_huawei_file: UploadFile = File(None),
#     dwdm_ciena_file: UploadFile = File(None),
#     dwdm_zte_file: UploadFile = File(None),
#     dwdm_nokia_file: UploadFile = File(None),

#     wan_file: UploadFile = File(...)
# ):

#     eci_path = save_file(eci_file) if eci_file else None
#     huawei_path = save_file(huawei_file) if huawei_file else None
#     ciena_path = save_file(ciena_file) if ciena_file else None
#     tejas_path = save_file(tejas_file) if tejas_file else None

#     dwdm_huawei_path = save_file(dwdm_huawei_file) if dwdm_huawei_file else None
#     dwdm_ciena_path = save_file(dwdm_ciena_file) if dwdm_ciena_file else None
#     dwdm_zte_path = save_file(dwdm_zte_file) if dwdm_zte_file else None
#     dwdm_nokia_path = save_file(dwdm_nokia_file) if dwdm_nokia_file else None

#     wan_path = save_file(wan_file)

#     output_path = process_main1(
#         eci_path,
#         huawei_path,
#         ciena_path,
#         tejas_path,
#         dwdm_huawei_path,
#         dwdm_ciena_path,
#         dwdm_zte_path,
#         dwdm_nokia_path,
#         wan_path,
#         circle
#     )

#     return FileResponse(output_path)

# #main2 
# from logic.main_logic.main2 import process_main2
# @app.post("/generate_main2")
# async def generate_main2(
#     circle: str = Form(...),
#     main1_file: UploadFile = File(...),
#     poc_file: UploadFile = File(None),
#     poc_router_file: UploadFile = File(None),
#     bsc_file: UploadFile = File(None)
# ):

#     main1_path = save_file(main1_file)
#     poc_path = save_file(poc_file) if poc_file else None
#     poc_router_path = save_file(poc_router_file) if poc_router_file else None
#     bsc_path = save_file(bsc_file) if bsc_file else None

#     output_path = process_main2(
#         main1_path,
#         poc_path,
#         poc_router_path,
#         bsc_path,
#         circle
#     )

#     return FileResponse(
#         output_path,
#         filename=os.path.basename(output_path),
#         media_type="text/csv"
#     ) 
    
    
    
    
    
# #Transform data
# from logic.transform_logic import run_transform

# @app.post("/transform")
# async def transform_output(
#     circle: str = Form(...),
#     transform_type: str = Form(...),
#     input_file: UploadFile = File(...)
# ):

#     input_path = save_file(input_file)

#     output_path = run_transform(input_path, circle, transform_type)

#     return FileResponse(
#         output_path,
#         filename=os.path.basename(output_path),
#         media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#     )


