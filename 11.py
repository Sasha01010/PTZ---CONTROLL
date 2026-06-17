#!/usr/bin/env python3

import cv2
import numpy as np
import mediapipe as mp
import mysql.connector
from mysql.connector import Error
import sys
import socket
import time
import threading
import queue
from PyQt6.QtWidgets import (QApplication, QMainWindow, QLabel, QPushButton,
QHBoxLayout, QVBoxLayout, QWidget, QGroupBox, QGridLayout, QStatusBar, 
QTabWidget, QTableWidget, QTableWidgetItem, QHeaderView, QListWidget, 
QListWidgetItem, QSizePolicy)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QImage, QPixmap

# ═══════════════════════════════════════════════════════════════
# 1. КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════════
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "ptz_system_db",
    "autocommit": True
}

CAMERAS_CONFIG = {
    "main": {
        "db_id": 1,
        "name": "Главная (Wide)",
        "ip": "172.16.232.77",
        "rtsp": "rtsp://admin:admin@172.16.232.77/stream/main",
        "visca_port": 52381
    },
    "med": {
        "db_id": 2,
        "name": "Средний (Med)",
        "ip": "192.168.1.101",
        "rtsp": "rtsp://admin:admin@192.168.1.101/stream/main",
        "visca_port": 52381
    },
    "close": {
        "db_id": 3,
        "name": "Крупный (Close)",
        "ip": "192.168.1.102",
        "rtsp": "rtsp://admin:admin@192.168.1.102/stream/main",
        "visca_port": 52381
    }
}

# ═══════════════════════════════════════════════════════════════
# 2. VISCA CONTROLLER
# ═══════════════════════════════════════════════════════════════
class ViscaController:
    def __init__(self, ip, port=52381):
        self.ip = ip
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(0.1)
        self.lock = threading.Lock()
        self.header = bytes([0x81])
        self.terminator = bytes([0xFF])
        self.last_cmd_time = 0.0
        self.min_interval = 0.15
        self.last_cmd_tuple = None

    def _send(self, cmd_bytes, force=False):
        with self.lock:
            now = time.time()
            if not force and self.last_cmd_tuple is not None:
                if now - self.last_cmd_time < self.min_interval:
                    return True 
            try:
                self.sock.sendto(self.header + cmd_bytes + self.terminator, (self.ip, self.port))
                self.last_cmd_time = now
                return True
            except Exception as e:
                print(f"VISCA Send Error: {e}")
                return False

    def move(self, direction, pan_speed=0x10, tilt_speed=0x10, force=False):
        codes = {
            'up': bytes([0x01, 0x06, 0x01, pan_speed, tilt_speed, 0x03, 0x01]),
            'down': bytes([0x01, 0x06, 0x01, pan_speed, tilt_speed, 0x03, 0x02]),
            'left': bytes([0x01, 0x06, 0x01, pan_speed, tilt_speed, 0x01, 0x03]),
            'right': bytes([0x01, 0x06, 0x01, pan_speed, tilt_speed, 0x02, 0x03]),
            'stop': bytes([0x01, 0x06, 0x01, pan_speed, tilt_speed, 0x03, 0x03])
        }
        if direction in codes:
            cmd_tuple = (direction, pan_speed, tilt_speed)
            if force or self.last_cmd_tuple != cmd_tuple or (time.time() - self.last_cmd_time >= self.min_interval):
                success = self._send(codes[direction], force=force)
                if success:
                    self.last_cmd_tuple = cmd_tuple

    def home(self):
        print(f" Отправка HOME команды на {self.ip}:{self.port}")
        self.last_cmd_tuple = None
        self._send(bytes([0x01, 0x06, 0x04]), force=True)

    def close(self):
        self.move('stop', force=True)
        try:
            self.sock.close()
        except:
            pass

# ═══════════════════════════════════════════════════════════════
# 3. БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════
class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.connected = False
        self.session_id = None
        self.log_queue = queue.Queue(maxsize=1000)
        self.init_database()
        threading.Thread(target=self._db_worker, daemon=True).start()

    def _db_worker(self):
        while True:
            try:
                data = self.log_queue.get(timeout=1)
                if data is None: break
                self._insert_log(data)
            except queue.Empty:
                continue
            except Exception as e:
                print(f"DB Worker Error: {e}")

    def _insert_log(self, data):
        if not self.connected: return
        try:
            conn = mysql.connector.connect(**self.config, connection_timeout=5)
            cur = conn.cursor()
            cur.execute("""INSERT INTO ptz_session_tracking
                (id_object, id_camera, id_session, event_time, confidence, bbox_x, bbox_y)
                VALUES (%s, %s, %s, NOW(), %s, %s, %s)""",
                (data['obj_id'], data['cam_id'], data['sess_id'], data['conf'], data['x'], data['y']))
            conn.commit()
            conn.close()
        except Error as e:
            print(f"Ошибка записи в БД: {e}")
            self.connected = False

    def init_database(self):
        try:
            conn = mysql.connector.connect(**self.config, connection_timeout=5)
            cur = conn.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {self.config['database']}")
            conn.commit()
            cur.execute("""CREATE TABLE IF NOT EXISTS ptz_session (
                id_session INT AUTO_INCREMENT PRIMARY KEY, start_date DATETIME)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS ptz_session_tracking (
                id INT AUTO_INCREMENT PRIMARY KEY, id_object INT, id_camera INT,
                id_session INT, event_time DATETIME, confidence FLOAT, bbox_x INT, bbox_y INT)""")
            conn.commit()
            cur.execute("INSERT INTO ptz_session (start_date) VALUES (NOW())")
            self.session_id = cur.lastrowid
            conn.commit()
            self.connected = True
            print(f"БД подключена. Session ID: {self.session_id}")
        except Error as e:
            print(f"Не удалось подключить БД: {e}")

    def log(self, obj_id, cam_id, conf, x, y):
        if self.connected and self.session_id:
            try:
                self.log_queue.put_nowait({
                    'obj_id': int(obj_id), 'cam_id': int(cam_id),
                    'sess_id': int(self.session_id), 'conf': float(conf),
                    'x': int(x), 'y': int(y)
                })
            except queue.Full:
                pass

    def get_stats(self, limit=50):
        if not self.connected: return []
        try:
            conn = mysql.connector.connect(**self.config, connection_timeout=5)
            cur = conn.cursor()
            cur.execute("""SELECT event_time, id_object, confidence, bbox_x, bbox_y
                FROM ptz_session_tracking ORDER BY id DESC LIMIT %s""", (limit,))
            result = cur.fetchall()
            conn.close()
            return result
        except Error as e:
            print(f"Ошибка чтения из БД: {e}")
            return []

# ═══════════════════════════════════════════════════════════════
# 4. ГИБРИДНЫЙ ТРЕКЕР (РЕШЕНИЕ ПРОБЛЕМЫ "ОТОШЕЛ И ПОТЕРЯЛСЯ")
# ═══════════════════════════════════════════════════════════════
class SmartFaceTracker:
    def __init__(self):
        self.tracked_faces = {}
        self.next_id = 1

    def calculate_iou(self, box1, box2):
        x1 = max(box1[0], box2[0])
        y1 = max(box1[1], box2[1])
        x2 = min(box1[0] + box1[2], box2[0] + box2[2])
        y2 = min(box1[1] + box1[3], box2[1] + box2[3])
        if x2 <= x1 or y2 <= y1: return 0.0
        intersection = (x2 - x1) * (y2 - y1)
        area1 = box1[2] * box1[3]
        area2 = box2[2] * box2[3]
        union = area1 + area2 - intersection
        return intersection / union if union > 0 else 0.0

    def is_match(self, prev_box, curr_box):
        # prev_box, curr_box: (x, y, w, h)
        prev_cx = prev_box[0] + prev_box[2] / 2
        prev_cy = prev_box[1] + prev_box[3] / 2
        curr_cx = curr_box[0] + curr_box[2] / 2
        curr_cy = curr_box[1] + curr_box[3] / 2
        
        # Нормализованное расстояние по центрам относительно размера ПРЕДЫДУЩЕГО лица
        dist_x = abs(curr_cx - prev_cx) / max(prev_box[2], 1)
        dist_y = abs(curr_cy - prev_cy) / max(prev_box[3], 1)
        
        
        if dist_x < 0.6 and dist_y < 0.6:
            return True
            
        # Запасной вариант: классический IoU (если лицо сдвинулось вбок, но размер тот же)
        return self.calculate_iou(prev_box, curr_box) > 0.2

    def update(self, detected_faces):
        current_frame = time.time()
        assigned_detections = set()
        result = []
        
        for face_id, (prev_x, prev_y, prev_w, prev_h, last_seen) in list(self.tracked_faces.items()):
            best_dist = float('inf')
            best_det_idx = -1
            
            for idx, (x, y, w, h) in enumerate(detected_faces):
                if idx in assigned_detections: continue
                
                if self.is_match((prev_x, prev_y, prev_w, prev_h), (x, y, w, h)):
                    # Вычисляем расстояние до центра для выбора наилучшего совпадения
                    prev_cx, prev_cy = prev_x + prev_w/2, prev_y + prev_h/2
                    curr_cx, curr_cy = x + w/2, y + h/2
                    dist = ((curr_cx - prev_cx)**2 + (curr_cy - prev_cy)**2)**0.5
                    
                    if dist < best_dist:
                        best_dist = dist
                        best_det_idx = idx
            
            if best_det_idx >= 0:
                x, y, w, h = detected_faces[best_det_idx]
                self.tracked_faces[face_id] = (x, y, w, h, current_frame)
                result.append((face_id, x, y, w, h))
                assigned_detections.add(best_det_idx)
            else:
                # Если детектор моргнул или лицо временно скрылось, ID не слетит.
                if current_frame - last_seen > 3.0:
                    del self.tracked_faces[face_id]
                    
        # Создаем новые ID для новых лиц
        for idx, (x, y, w, h) in enumerate(detected_faces):
            if idx not in assigned_detections:
                face_id = self.next_id
                self.next_id += 1
                self.tracked_faces[face_id] = (x, y, w, h, current_frame)
                result.append((face_id, x, y, w, h))
                
        self.tracked_faces = {fid: data for fid, data in self.tracked_faces.items() if current_frame - data[4] < 3.0}
        return result

# ═══════════════════════════════════════════════════════════════
# 5. ПОТОК КАМЕРЫ
# ═══════════════════════════════════════════════════════════════
class CameraWorker(QThread):
    frame_signal = pyqtSignal(object, str)
    faces_signal = pyqtSignal(list, str)
    status_signal = pyqtSignal(str)

    def __init__(self, cam_id, config, db):
        super().__init__()
        self.cam_id = cam_id
        self.config = config
        self.db = db
        self.visca = ViscaController(config['ip'])
        self.running = False
        self.face_tracker = SmartFaceTracker()
        self.mp_face = mp.solutions.face_detection
        self.detector = self.mp_face.FaceDetection(model_selection=1, min_detection_confidence=0.25)
        
        self.tracking_active = False
        self.target_face_id = -1
        self.manual_moving = False 
        
        self.alpha = 0.12  # Плавное сглаживание
        self.deadzone_x = 0.10  # 10% мертвая зона
        self.deadzone_y = 0.10
        self.consecutive_out_of_zone = 0
        self.required_frames_to_move = 4  # ~0.13 сек подтверждения
        
        self.smooth_x = 0.0
        self.smooth_y = 0.0
        self.last_known_position = None
        self.lost_frames = 0
        self.max_lost_frames = 90  # 3 секунды памяти перед сдачей
        self.search_mode = False

    def run(self):
        self.running = True
        cap = cv2.VideoCapture(self.config['rtsp'], cv2.CAP_FFMPEG)
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        cap.set(cv2.CAP_PROP_FPS, 30)
        frame_count = 0
        
        while self.running:
            ret, frame = cap.read()
            if not ret:
                time.sleep(0.1)
                continue
            
            frame_count += 1
            h, w, _ = frame.shape
            output_frame = frame.copy()
            current_faces = []
            
            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            results = self.detector.process(rgb)
            detected_faces = []
            
            if results.detections:
                for det in results.detections:
                    bbox = det.location_data.relative_bounding_box
                    x, y = int(bbox.xmin * w), int(bbox.ymin * h)
                    bw, bh = int(bbox.width * w), int(bbox.height * h)
                    detected_faces.append((x, y, bw, bh))
                    
            tracked_faces = self.face_tracker.update(detected_faces)
            target_found = False
            
            for face_id, x, y, bw, bh in tracked_faces:
                cx, cy = x + bw // 2, y + bh // 2
                current_faces.append({'id': face_id, 'x': cx, 'y': cy, 'w': bw, 'h': bh})
                
                color = (0, 255, 0)
                if self.tracking_active and face_id == self.target_face_id:
                    color = (0, 0, 255)
                    target_found = True
                    self.lost_frames = 0
                    self.last_known_position = (cx, cy, bw, bh)
                    self.search_mode = False
                    
                    cv2.rectangle(output_frame, (x, y), (x+bw, y+bh), color, 3)
                    
                    error_x = (cx - w/2) / (w/2)
                    error_y = (cy - h/2) / (h/2)
                    
                    self.smooth_x = self.alpha * error_x + (1 - self.alpha) * self.smooth_x
                    self.smooth_y = self.alpha * error_y + (1 - self.alpha) * self.smooth_y
                    
                    is_out_of_zone = (abs(self.smooth_x) > self.deadzone_x) or (abs(self.smooth_y) > self.deadzone_y)
                    
                    if is_out_of_zone:
                        self.consecutive_out_of_zone += 1
                        if self.consecutive_out_of_zone >= self.required_frames_to_move:
                            max_speed = 14  # Безопасная, но достаточная скорость
                            speed_pan = int(np.clip(abs(self.smooth_x) * 25, 4, max_speed))
                            speed_tilt = int(np.clip(abs(self.smooth_y) * 25, 4, max_speed))
                            
                            if abs(self.smooth_x) > abs(self.smooth_y):
                                direction = 'right' if self.smooth_x > 0 else 'left'
                            else:
                                direction = 'down' if self.smooth_y > 0 else 'up'
                            
                            self.visca.move(direction, speed_pan, speed_tilt)
                    else:
                        self.consecutive_out_of_zone = 0
                        self.visca.move('stop')
                        
                    if frame_count % 10 == 0:
                        self.db.log(face_id, self.config['db_id'], 0.9, cx, cy)
                else:
                    cv2.rectangle(output_frame, (x, y), (x+bw, y+bh), color, 2)

            if self.tracking_active and not target_found:
                self.lost_frames += 1
                if self.lost_frames >= self.max_lost_frames:
                    self.status_signal.emit(f"Цель потеряна! Выберите новую.")
                    self.tracking_active = False
                    self.target_face_id = -1
                    self.visca.move('stop')
                else:
                    # Пока ждем возвращения, камера стоит на месте, не крутится наугад
                    self.visca.move('stop')
            
            elif not self.tracking_active and not self.manual_moving:
                self.visca.move('stop')

            self.frame_signal.emit(output_frame, self.cam_id)
            self.faces_signal.emit(current_faces, self.cam_id)
            
        cap.release()
        self.visca.close()

    def stop(self):
        self.running = False
        self.wait()

# ═══════════════════════════════════════════════════════════════
# 6. ГЛАВНОЕ ОКНО
# ═══════════════════════════════════════════════════════════════
class PTZApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("🎥 PTZ Array Controller | Stable & Responsive")
        self.resize(1400, 900)
        self.db = DatabaseManager(DB_CONFIG)
        self.workers = {}
        self.faces_cache = {}
        self.lists = {}
        self.init_ui()
        self.start_workers()
        
        self.db_timer = QTimer()
        self.db_timer.timeout.connect(self.update_db_table)
        self.db_timer.start(2000)

    def init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        
        video_container = QWidget()
        video_layout = QGridLayout(video_container)
        video_layout.setSpacing(5)
        self.labels = {}
        
        for i, (cid, cfg) in enumerate(CAMERAS_CONFIG.items()):
            lbl = QLabel()
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            lbl.setStyleSheet("background:#000; color:#fff; border: 2px solid #333;")
            lbl.setText(f"📡 {cfg['name']}")
            lbl.setMinimumSize(300, 200)
            lbl.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
            self.labels[cid] = lbl
            video_layout.addWidget(lbl, 0, i)
            
        main_layout.addWidget(video_container, stretch=4)
        
        tabs = QTabWidget()
        control_tab = QWidget()
        ctrl_layout = QHBoxLayout(control_tab)
        
        for cid, cfg in CAMERAS_CONFIG.items():
            panel = QGroupBox(cfg['name'])
            pl = QVBoxLayout(panel)
            
            self.lists[cid] = QListWidget()
            self.lists[cid].setMaximumHeight(150)
            self.lists[cid].itemClicked.connect(lambda item, cam_id=cid: self.select_face(cam_id, item))
            pl.addWidget(QLabel("Обнаруженные лица:"))
            pl.addWidget(self.lists[cid])
            
            ptz_grid = QGridLayout()
            btn_style = "QPushButton{padding:10px; font-weight:bold; background:#444; color:white; border-radius:5px;} QPushButton:pressed{background:#2a7d3b;}"
            
            b_up = QPushButton("⬆️")
            b_up.setStyleSheet(btn_style)
            b_up.pressed.connect(lambda checked=False, cam_id=cid: self.manual_move_start(cam_id, 'up'))
            b_up.released.connect(lambda checked=False, cam_id=cid: self.manual_move_stop(cam_id))
            ptz_grid.addWidget(b_up, 0, 1)
            
            b_left = QPushButton("⬅️")
            b_left.setStyleSheet(btn_style)
            b_left.pressed.connect(lambda checked=False, cam_id=cid: self.manual_move_start(cam_id, 'left'))
            b_left.released.connect(lambda checked=False, cam_id=cid: self.manual_move_stop(cam_id))
            ptz_grid.addWidget(b_left, 1, 0)
            
            b_home = QPushButton("🏠")
            b_home.setStyleSheet(btn_style)
            b_home.clicked.connect(lambda checked=False, cam_id=cid: self.go_home(cam_id))
            ptz_grid.addWidget(b_home, 1, 1)
            
            b_right = QPushButton("➡️")
            b_right.setStyleSheet(btn_style)
            b_right.pressed.connect(lambda checked=False, cam_id=cid: self.manual_move_start(cam_id, 'right'))
            b_right.released.connect(lambda checked=False, cam_id=cid: self.manual_move_stop(cam_id))
            ptz_grid.addWidget(b_right, 1, 2)
            
            b_down = QPushButton("⬇️")
            b_down.setStyleSheet(btn_style)
            b_down.pressed.connect(lambda checked=False, cam_id=cid: self.manual_move_start(cam_id, 'down'))
            b_down.released.connect(lambda checked=False, cam_id=cid: self.manual_move_stop(cam_id))
            ptz_grid.addWidget(b_down, 2, 1)
            
            pl.addLayout(ptz_grid)
            
            btn_track = QPushButton("🔴 Трекинг ВЫКЛ")
            btn_track.setCheckable(True)
            btn_track.setStyleSheet(btn_style)
            btn_track.clicked.connect(lambda checked, cam_id=cid, btn=btn_track: self.toggle_tracking(cam_id, checked, btn))
            pl.addWidget(btn_track)
            
            ctrl_layout.addWidget(panel)
            
        tabs.addTab(control_tab, "🎮 Управление и Трекинг")
        
        db_tab = QWidget()
        db_layout = QVBoxLayout(db_tab)
        refresh_btn = QPushButton("🔄 Обновить данные из БД")
        refresh_btn.setStyleSheet("""
            QPushButton { padding: 12px; background: #2a7d3b; color: white; font-weight: bold; font-size: 14px; border-radius: 6px; }
            QPushButton:hover { background: #3a9d4b; }
            QPushButton:pressed { background: #1a5d2b; }
        """)
        refresh_btn.clicked.connect(self.update_db_table)
        db_layout.addWidget(refresh_btn)
        
        self.db_table = QTableWidget()
        self.db_table.setColumnCount(5)
        self.db_table.setHorizontalHeaderLabels(["Время", "Объект ID", "Confidence", "X", "Y"])
        self.db_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.db_table.setStyleSheet("""
            QTableWidget { background: #252525; border: 1px solid #444; color: #fff; }
            QHeaderView::section { background: #333; color: #fff; padding: 5px; border: 1px solid #444; }
        """)
        db_layout.addWidget(self.db_table)
        tabs.addTab(db_tab, "📊 База данных")
        
        main_layout.addWidget(tabs, stretch=1)

    def start_workers(self):
        for cid, cfg in CAMERAS_CONFIG.items():
            w = CameraWorker(cid, cfg, self.db)
            w.frame_signal.connect(lambda frame, cam_id=cid: self.update_frame(frame, cam_id))
            w.faces_signal.connect(lambda faces, cam_id=cid: self.update_faces_list(faces, cam_id))
            w.status_signal.connect(self.statusBar().showMessage)
            w.start()
            self.workers[cid] = w

    def update_frame(self, frame, cam_id):
        if cam_id in self.labels:
            h, w, c = frame.shape
            img = QImage(frame.data, w, h, 3*w, QImage.Format.Format_BGR888)
            self.labels[cam_id].setPixmap(QPixmap.fromImage(img).scaled(
                self.labels[cam_id].size(), Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation))

    def update_faces_list(self, faces, cam_id):
        self.faces_cache[cam_id] = faces
        if cam_id in self.lists:
            lst = self.lists[cam_id]
            lst.clear()
            for f in faces:
                item = QListWidgetItem(f"👤 ID:{f['id']}")
                if self.workers[cam_id].tracking_active and self.workers[cam_id].target_face_id == f['id']:
                    item.setText(f"✅ {item.text()} [ВЫБРАН]")
                    item.setForeground(Qt.GlobalColor.yellow)
                lst.addItem(item)

    def select_face(self, cam_id, item):
        try:
            text = item.text()
            id_str = text.split("ID:")[1].split(" ")[0]
            face_id = int(id_str)
            w = self.workers[cam_id]
            w.tracking_active = True
            w.manual_moving = False
            w.target_face_id = face_id
            w.lost_frames = 0
            w.consecutive_out_of_zone = 0
            print(f"✅ Трекинг лица #{face_id} на камере {cam_id}")
        except: pass

    def toggle_tracking(self, cam_id, checked, btn):
        w = self.workers[cam_id]
        w.tracking_active = checked
        w.manual_moving = False
        if checked:
            btn.setText("🟢 Трекинг ВКЛ")
            btn.setStyleSheet("background:#2a7d3b; color:white; font-weight:bold;")
        else:
            btn.setText("🔴 Трекинг ВЫКЛ")
            btn.setStyleSheet("background:#444; color:white; font-weight:bold;")
            w.target_face_id = -1
            w.visca.move('stop', force=True)
            w.lost_frames = 0

    def manual_move_start(self, cam_id, direction):
        if cam_id in self.workers:
            w = self.workers[cam_id]
            w.tracking_active = False
            w.manual_moving = True
            w.visca.move(direction, 0x14, 0x10, force=True)

    def manual_move_stop(self, cam_id):
        if cam_id in self.workers:
            w = self.workers[cam_id]
            w.manual_moving = False
            w.visca.move('stop', force=True)

    def go_home(self, cam_id):
        if cam_id in self.workers:
            w = self.workers[cam_id]
            w.tracking_active = False
            w.manual_moving = False
            w.target_face_id = -1
            w.visca.move('stop', force=True)
            time.sleep(0.1)
            w.visca.home()
            self.statusBar().showMessage(f"🏠 Камера {cam_id} возвращается в Home...", 3000)

    def update_db_table(self):
        stats = self.db.get_stats(50)
        self.db_table.setRowCount(0)
        self.db_table.setRowCount(len(stats))
        
        for r, row_data in enumerate(stats):
            t, oid, conf, x, y = row_data
            time_str = str(t)[:19] if t else "N/A"
            conf_str = f"{float(conf):.2f}" if conf is not None else "0.00"
            x_str = str(int(x)) if x is not None else "0"
            y_str = str(int(y)) if y is not None else "0"
            
            self.db_table.setItem(r, 0, QTableWidgetItem(time_str))
            self.db_table.setItem(r, 1, QTableWidgetItem(str(int(oid)) if oid else "0"))
            self.db_table.setItem(r, 2, QTableWidgetItem(conf_str))
            self.db_table.setItem(r, 3, QTableWidgetItem(x_str))
            self.db_table.setItem(r, 4, QTableWidgetItem(y_str))
            
        self.statusBar().showMessage(f"📊 БД обновлена. Записей: {len(stats)}", 3000)

    def closeEvent(self, event):
        for w in self.workers.values():
            w.stop()
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    window = PTZApp()
    window.show()
    sys.exit(app.exec())
