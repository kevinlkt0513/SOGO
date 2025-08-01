const hccEventType = "NORMAL_DEDUCT_POINT";
const memberTypes = ["SOGO_VIP", "SOGO_VVIP"];
const appId = "A000111222";
const now = new Date();

db.events.aggregate([

  // 🧊 1. 時間條件與活動類型初步過濾
  {
    $match: {
      eventStatus: "USING",                // 活動必須是啟用狀態
      onlyApp: true,                       // 僅限 App 活動
      hccEventType: hccEventType,          // 活動類型符合輸入
      startDate: { $lte: now },            // 活動已開始
      endDate: { $gte: now },              // 活動尚未結束
      $or: [
        { hccEventType: { $ne: "CLUB_DEDUCT_POINT" } },       // 非 CLUB 類型
        { memberTypes: { $in: memberTypes } }                 // 或符合會員類型
      ]
    }
  },

  // 🧩 2. 使用者是否在名單內：用 LEFT JOIN 方式找出是否為參與者
  {
    $lookup: {
      from: "event_attendees",
      let: { eventNo: "$eventNo" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$eventNo", "$$eventNo"] },
                { $eq: ["$appId", appId] }
              ]
            }
          }
        },
        { $limit: 1 }
      ],
      as: "matchedAttendee"
    }
  },

  // ✅ 3. 根據會員開放與參加者名單進行資格過濾
  {
    $match: {
      $or: [
        { allMember: true },                       // 若為全會員皆可參與
        { "matchedAttendee.0": { $exists: true } } // 否則需 appId 存在於參加者名單
      ]
    }
  },

  // 📦 4. 將 usingBranchIds 陣列拆散為單一列（每列一個分館代碼）
  { $unwind: "$usingBranchIds" },
  { $unwind: "$usingBranchNames" },

  // 📌 5. 聚合唯一的 branchId / branchName，避免重覆
  {
    $group: {
      _id: "$usingBranchIds",
      branchName: { $first: "$usingBranchNames" }
    }
  },

  // 🧾 6. 整理輸出欄位格式
  {
    $project: {
      _id: 0,
      branchId: "$_id",
      branchName: "$branchName"
    }
  }

]).toArray()



