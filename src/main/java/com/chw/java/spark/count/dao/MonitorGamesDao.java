package com.chw.java.spark.count.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.chw.java.spark.count.entity.MonitorGames;
import com.chw.java.spark.count.util.MysqlUtils;

public class MonitorGamesDao {
	/**
	 * 获取所有监视游戏
	 * 
	 * @return
	 */
	public static List<MonitorGames> getMonitorGames() {
		return MysqlUtils.queryByBeanListHandler("select * from monitor_games", MonitorGames.class);
	}

	/**
	 * 以[game_id -> monitorGame]的形式获取所有监视游戏
	 * 
	 * @return
	 */
	public static Map<Integer, MonitorGames> getMapMonitorGames() {
		Map<Integer, MonitorGames> mem = new HashMap<>();

		for (MonitorGames monitorGame : MysqlUtils.queryByBeanListHandler("select * from monitor_games",
				MonitorGames.class)) {
			mem.put(monitorGame.game_id, monitorGame);
		}
		return mem;
	}
}
