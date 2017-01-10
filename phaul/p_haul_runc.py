#
# Runc container hauler
#

import json
import logging
import os
import re
import subprocess as sp
import util

import pycriu

import criu_cr
import fs_haul_subtree


# Some constants for runc
runc_bin = "/usr/bin/runc"  # "/usr/local/sbin/runc" for compiled version
runc_run = "/var/run/runc/"
runc_conf_name = "config.json"


class p_haul_type(object):
	def __init__(self, ctid):

		# Validate provided container ID
		if (not(re.match("^[\w-]+$", ctid)) or len(ctid) > 1024):
			raise Exception("Invalid runc container name: %s", ctid)

		self._ctid = ctid
		self._veths = []    #FIXME add values if connection
		self._binds = {}

	def _get_cgroup_list(self):
		cgroup_list = []
		with open("/proc/self/cgroup", "r") as proc_cgroups:
			for line in proc_cgroups.readlines():
				parts = line.split(":")
				if len(parts) < 3:
					logging.error("Invalid cgroup entry %s found",
							line)
				else:
					cgroup_list.append(re.sub("name=",
							"", parts[1]))
		return cgroup_list

	def init_src(self):
		self._bridged = True
		try:
			with open(runc_run + self._ctid + "/state.json",
					"r") as state:
				self._container_state = json.loads(state.read())
			self._labels = self._container_state["config"]["labels"]
			self._ct_rootfs = self._container_state["config"]["rootfs"]
			self._root_pid = self._container_state["init_process_pid"]
			self._ext_descriptors = json.dumps(
						self._container_state["external_descriptors"])
		except IOError:
			raise Exception(
				"Unable to get container data, check if %s is running",
				self._ctid)
		except KeyError:
			raise Exception("Invalid container state retrieved")

		self._runc_bundle = next(label[len("bundle="):]
						for label in self._labels
						if label.startswith("bundle="))

		if any([mount["device"] == "cgroup" for mount in
				self._container_state["config"]["mounts"]]):
			cgroups = self._get_cgroup_list()

		for mount in self._container_state["config"]["mounts"]:
			if mount["device"] == "bind":
				if mount["destination"].startswith(self._ct_rootfs):
					dst = mount["destination"][len(self._ct_rootfs):]
				else:
					dst = mount["destination"]
				self._binds.update({dst: dst})
			if mount["device"] == "cgroup":
				for cgroup in cgroups:
					dst = os.path.join(mount["destination"], cgroup)
					if dst.startswith(self._ct_rootfs):
						dst = dst[len(self._ct_rootfs):]
					self._binds.update({dst: dst})

		self.__load_ct_config(self._runc_bundle)
		logging.info("Container rootfs: %s", self._ct_rootfs)

	def init_dst(self):
		self._bridged = False

	def adjust_criu_req(self, req):
		if req.type == pycriu.rpc.DUMP:
			req.opts.root = self._ct_rootfs
			req.opts.manage_cgroups = True
			req.opts.notify_scripts = True
			for key, value in self._binds.items():
				req.opts.ext_mnt.add(key=key, val=value)

		if req.type == pycriu.rpc.RESTORE:
			req.opts.rst_sibling = True

	def root_task_pid(self):
		return self._root_pid

	def __load_ct_config(self, path):
		self._ct_config = os.path.join(self._runc_bundle, runc_conf_name)
		logging.info("Container config: %s", self._ct_config)

	def set_options(self, opts):
		pass

	def mount(self):
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
		with open(bundle_filename, "w+") as bundle_file:
			bundle_file.write(self._runc_bundle)
		desc_path = os.path.join(path, "descriptors.json")
		with open(desc_path, "w+") as desc_file:
			desc_file.write(self._ext_descriptors)
		return [(bundle_filename, "bundle.txt"),
				(desc_path, "descriptors.json")]

	def put_meta_images(self, dir):
		with open(os.path.join(dir, "bundle.txt")) as bundle_file:
			self._runc_bundle = bundle_file.read()

	def final_dump(self, pid, img, ccon, fs):
		criu_cr.criu_dump(self, pid, img, ccon, fs)

	def migration_complete(self, fs, target_host):
		ret = sp.call([runc_bin, "kill", self._ctid])
		ret = sp.call([runc_bin, "delete", self._ctid])

	def migration_fail(self, fs):
		pass

	def target_cleanup(self, src_data):
		pass

	def final_restore(self, img, criu):
		#criu_cr.criu_restore(self, img, connection)
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
		return True

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
