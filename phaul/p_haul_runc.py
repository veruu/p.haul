#
# Runc container hauler
#

import json
import logging
import os
import re
import subprocess as sp
import util

import fs_haul_subtree


# Some constants for runc
runc_bin = "/usr/bin/runc"  # "/usr/local/sbin/runc" for compiled version
runc_conf_name = "config.json"


class p_haul_type(object):
	def __init__(self, ctid):

		# Validate provided container ID
		if (not(re.match("^[\w-]+$", ctid)) or len(ctid) > 1024):
			raise Exception("Invalid runc container name: %s", ctid)

		self._ctid = ctid
		self._veths = []    #FIXME add values if connection -- how to find out?

	def init_src(self):
		self._bridged = True
		try:
			self._container_state = json.loads(sp.check_output([runc_bin,
								"state",
								self._ctid]))
			self._runc_bundle = self._container_state["bundlePath"]
			self._ct_rootfs = self._container_state["rootfsPath"]
			self._root_pid = self._container_state["pid"]
		except sp.CalledProcessError:
			raise Exception(
				"Unable to get container data, check if %s is running",
				self._ctid)
		except KeyError:
			raise Exception("Invalid container state retrieved")

		self.__load_ct_config(self._runc_bundle)
		logging.info("Container rootfs: %s", self._ct_rootfs)

	def init_dst(self):
		self._bridged = False

	def adjust_criu_req(self, req):
		pass

	def root_task_pid(self):
		return self._root_pid

	def __load_ct_config(self, path):
		self._ct_config = os.path.join(self._runc_bundle, runc_conf_name)
		logging.info("Container config: %s", self._ct_config)

	def set_options(self, opts):
		pass

	def umount(self):
		pass

	def start(self):
		pass

	def stop(self, umount):
		pass

	def get_fs(self, fdfs=None):
		return fs_haul_subtree.p_haul_fs([self._ct_rootfs, self._ct_config])

	def get_fs_receiver(self, fdfs=None):
		return None

	def get_meta_images(self, path):
		bundle_filename = os.path.join(path, "bundle.txt")
		with open(bundle_filename, 'w+') as bundle_file:
			bundle_file.write(self._runc_bundle)
		desc_path = os.path.join(path, "descriptors.json")
		return [(bundle_filename, "bundle.txt"),
				(desc_path, "descriptors.json")]

	def put_meta_images(self, dir):
		with open(os.path.join(dir, "bundle.txt")) as bundle_file:
			self._runc_bundle = bundle_file.read()

	def final_dump(self, pid, img, ccon, fs):
		logging.info("Dump runc container %s", pid)
		image_path = "--image-path=" + img.image_dir()

		logf = open("/tmp/runc_checkpoint.log", "w+")
		ret = sp.call([runc_bin,
				"checkpoint",
				"--tcp-established",
				image_path,
				self._ctid],
				stdout=logf,
				stderr=logf)
		if ret:
			raise Exception("runc checkpoint failed")

	def migration_complete(self, fs, target_host):
		pass

	def migration_fail(self, fs):
		pass

	def target_cleanup(self, src_data):
		pass

	def final_restore(self, img, criu):
		logf = open("/tmp/runc_restore.log", "w+")
		bundle = "--bundle=" + self._runc_bundle
		image_path = "--image-path=" + img.image_dir()

		ret = sp.call([runc_bin,
				"restore",
				"-d",
				"--tcp-established",
				bundle,
				image_path,
				self._ctid],
				stdout=logf,
				stderr=logf)
		if ret:
			raise Exception("runc restore failed")

	def can_pre_dump(self):
		return True

	def dump_need_page_server(self):
		return False

	def can_migrate_tcp(self):
		return True

	def net_lock(self):
		for veth in self._veths:
			util.ifdown(veth.pair)

	def net_unlock(self):
		for veth in self._veths:
			util.ifup(veth.pair)
			if veth.link and not self._bridged:
				util.bridge_add(veth.pair, veth.link)
