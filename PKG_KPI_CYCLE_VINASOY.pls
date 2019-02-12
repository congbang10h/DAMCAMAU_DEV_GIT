create or replace PACKAGE BODY             PKG_KPI_CYCLE AS

  PROCEDURE GET_LIST_STAFF_KPI(res OUT SYS_REFCURSOR , shopId NUMBER, cycleId NUMBER) AS
    strSQL CLOB;
    in_clause CLOB;
    
    CURSOR c_title IS
      select k.kpi_id
      from kpi k
      where PLAN_TYPE = 2 --co thiet lap chi tieu
      order by ORDER_INDEX;
  BEGIN
    for x in c_title
    loop
        in_clause := in_clause  || to_char(x.kpi_id) || ', ';
    end loop;
    
    in_clause := substr(in_clause, 1, length(in_clause) - 2); -- bo dau ',' cuoi cung
    strSQL:= 
      'with lstStaff as (
        select st.staff_id, st.shop_id, k.kpi_id
        from STAFF st
        join STAFF_TYPE ste on st.staff_type_id = ste.staff_type_id
        join kpi k on k.plan_type = 2
        where st.shop_id = ' || shopId || '
          and st.status = 1
          and ste.specific_type = 1
      )
      , lstKpiInfo as (
        select nvl(k.kpi_id, 0) kpi_id, nvl(k.staff_id, 0) staff_id, nvl(k.shop_id, 0) shop_id
        , nvl(ki.cycle_id, ' || cycleId || ') cycle_id , ki.plan
        from lstStaff k
        left join KPI_IMPLEMENT ki on k.kpi_id = ki.kpi_id
          and k.staff_id = ki.staff_id
          and ki.shop_id = ' || shopId || '
          and ki.cycle_id =  ' || cycleId || '
      )
      , avgKpi as (
        select ki.kpi_id, 0 staff_id, 0 shop_id, 0 cycle_id, round(avg(nvl(plan, 0)), 2) plan
        from lstKpiInfo ki
        group by ki.kpi_id
      )
      , fullRows as (
        select ki.kpi_id, ki.staff_id staffId, ki.shop_id shopId, ki.cycle_id cycleId, ki.plan
        from lstKpiInfo ki
        union all
        select aKi.kpi_id, aKi.staff_id staffId, aKi.shop_id shopId, aKi.cycle_id cycleId, aKi.plan
        from avgKpi aKi
      )
      , pivotData as (
        select * 
        from (
          select info.kpi_id, info.staffId, info.shopId, info.cycleId, info.plan
          from fullRows info
        )
        PIVOT (max(plan) FOR kpi_id IN(' || in_clause || ') )
      )
      , fullInfo as (
        select sh.shop_code shopCode, sh.shop_name shopName
          , st.staff_code staffCode, st.staff_name staffName
          , (to_char(c.num, ''00'') || ''/'' || to_char(extract(year from c.year)) ) as cycleText
          , pi.*
        from pivotData pi
        left join SHOP sh on pi.shopId = sh.shop_id
        left join STAFF st on pi.staffId = st.staff_id
        left join CYCLE c on c.cycle_id = ' || cycleId || '
      )
      select * from fullInfo
      order by staffCode';
      
    -- dbms_output.put_line('in_clause = ' || in_clause);
    -- dbms_output.put_line('SQL = ' || strSQL);

    OPEN res FOR strSQL;
  END GET_LIST_STAFF_KPI;
  
  PROCEDURE P_KPI_REGISTER_HIS_FCYCLE (
    i_input_date date default trunc(sysdate)
  )
    /*
      @Procedure: Ghi nh?n th�ng tin ??ng k� KPI cho [CHU K?];
      @author: thuattq1
      
      @params:  
      i_input_date : Ng�y c?n t?ng h?p th�ng tin ??ng k� (chu?n nh?t l� ng�y cu?i chu k? mu?n ghi nh?n KPI).
    */
  as  
    i_kpi_period KPI_REGISTER.kpi_period%type := 1;-- 1 chu ky; 2: nam;
    
    vv_run_date date;
    vv_kpi_date CYCLE.end_date%type;
    vv_bccycle_date CYCLE.end_date%type; -- ngay dau chu ky hien tai
    vv_eccycle_date CYCLE.end_date%type; -- ngay cuoi chu ky hien tai
    
    vv_period_value KPI_QUOTA.kpi_period_value%type;
    vv_cperiod_value KPI_QUOTA.kpi_period_value%type; -- chu ky hien tai
    vv_is_curent number(1);
  begin
    vv_run_date := sysdate;
    select ce.cycle_id, trunc(ce.begin_date), trunc(ce.end_date)
    into vv_cperiod_value, vv_bccycle_date, vv_eccycle_date
    from CYCLE ce
    where ce.status = 1
      and ce.begin_date < trunc(vv_run_date) + 1
      and ce.end_date >= trunc(vv_run_date);
    
    -- neu: ngay dau chu ky hien tai <= i_input_date < trunc(ngay cuoi chu ky hien tai) + 1: chay cho sysdate
    -- else: chay cho ngay cuoi chu ky i_input_date;
    if (  i_input_date >= vv_bccycle_date
      and i_input_date  < vv_eccycle_date + 1)
    then
      vv_is_curent    := 1;
      vv_kpi_date     := trunc(vv_run_date);
      vv_period_value := vv_cperiod_value;
    else
      vv_is_curent := 0;
      
      select ce.cycle_id, trunc(ce.end_date)
      into vv_period_value, vv_kpi_date
      from CYCLE ce
      where ce.status = 1
        and ce.begin_date < trunc(i_input_date) + 1
        and ce.end_date >= trunc(i_input_date);
    end if;

    MERGE INTO KPI_REGISTER_HIS d
    USING (
      select krr.kpi_register_id, krr.object_type, krr.object_id, krr.kpi_period
        , kgcg.kpi_group_config_id, kgcg.code as group_code, kgcg.name as group_name, kgcg.kpi_group_type
        , kcg.kpi_config_id, kcg.code, kcg.name, kcg.update_type, kcg.order_index, kcg.plan_type
        , kte.procedure_code, kte.code as kpi_type_code, kte.kpi_type_id
        , kgdl.weighted, kgdl.max_value
      from KPI_REGISTER krr
      join KPI_GROUP_CONFIG kgcg 
      on krr.kpi_group_config_id = kgcg.kpi_group_config_id
        and kgcg.status = 1
      join KPI_GROUP_DETAIL kgdl
      on kgcg.kpi_group_config_id = kgdl.kpi_group_config_id
        and ((vv_is_curent = 1 and kgdl.status = 1) 
          or (vv_is_curent = 0 and kgdl.status in (0, 1))) -- qua khu chay ca trang thai 0
        and kgdl.from_kpi_period_value <= vv_period_value
        and (kgdl.to_kpi_period_value  >= vv_period_value 
          or kgdl.to_kpi_period_value is null)
      join KPI_CONFIG kcg
      on kgdl.kpi_config_id =kcg.kpi_config_id
        and kcg.status = 1     
        and kcg.update_type = 2 -- loai t? ??ng
        and kcg.kpi_period = 1 -- chu ky
      join KPI_TYPE kte
      on kte.kpi_type_id = kcg.kpi_type_id 
        and kte.status = 1
        and trim(kte.procedure_code) is not null
      where kgcg.kpi_group_type = 1 -- chu ky
        and ((vv_is_curent = 0 and krr.status in (0, 1))
          or (vv_is_curent = 1 and krr.status = 1))
        and krr.kpi_period = i_kpi_period
          and krr.from_date < trunc(vv_kpi_date) + 1
          and (krr.to_date >= trunc(vv_kpi_date) or krr.to_date is null)
    ) s
    ON (  d.kpi_period        = i_kpi_period
      and d.kpi_period_value  = vv_period_value
      and d.kpi_register_id   = s.kpi_register_id
      and d.object_type       = s.object_type
      and d.object_id         = s.object_id
      and d.kpi_group_config_id = s.kpi_group_config_id
      and d.kpi_config_id     = s.kpi_config_id
      and d.kpi_type_id       = s.kpi_type_id
      and d.procedure_code    = s.procedure_code
    ) 
    WHEN MATCHED THEN
    UPDATE SET
      -- d.kpi_group_config_id = s.kpi_group_config_id,
      d.kpi_group_code = s.group_code,
      d.kpi_group_name = s.group_name,
      d.kpi_group_type = s.kpi_group_type,
      -- d.kpi_config_id = s.kpi_config_id,
      d.kpi_config_code = s.code,
      d.kpi_config_name = s.name,
      d.plan_type = s.plan_type,
      -- d.kpi_type_id = s.kpi_type_id,
      d.kpi_type_code = s.kpi_type_code,
      -- d.procedure_code = s.procedure_code,
      d.weighted = s.weighted,
      d.max_value = s.max_value,
      d.run_date = vv_run_date,
      d.update_date = vv_run_date,
      d.update_user = 'SYS'
    WHEN NOT MATCHED THEN
    INSERT (
      d.kpi_register_his_id, d.kpi_period, d.kpi_period_value,
      d.kpi_register_id, d.object_type, d.object_id,
      d.kpi_group_config_id, d.kpi_group_code, d.kpi_group_name,
      d.kpi_group_type, d.kpi_config_id, d.kpi_config_code,
      d.kpi_config_name, d.plan_type, d.kpi_type_id,
      d.kpi_type_code, d.procedure_code, d.weighted,
      d.max_value, d.run_date, d.create_date,
      d.create_user)
    VALUES (
      seq_nextval_on_demand('KPI_REGISTER_HIS_SEQ'), i_kpi_period, vv_period_value,
      s.kpi_register_id, s.object_type, s.object_id,
      s.kpi_group_config_id, s.group_code, s.group_name,
      s.kpi_group_type, s.kpi_config_id, s.code,
      s.name, s.plan_type, s.kpi_type_id,
      s.kpi_type_code, s.procedure_code, s.weighted,
      s.max_value, vv_run_date, vv_run_date,
      'SYS');
    
    /* X�a d�ng d? li?u t?ng h?p th?a:
       Tr??ng h?p: ko t?n t?i b? ??ng k� h?p l� (KPI_REGISTER, KPI_GROUP_CONFIG, KPI_GROUP_DETAIL, KPI_CONFIG)*/
    delete RPT_KPI_CYCLE rpt
    where (rpt.kpi_register_id, rpt.cycle_id, rpt.kpi_group_config_id, rpt.kpi_config_id) in (
        select d.kpi_register_id
          --, d.kpi_period-- , d.kpi_type_id
          , d.kpi_period_value
          , d.kpi_group_config_id, d.kpi_config_id
        from KPI_REGISTER_HIS d
        where d.kpi_period        = i_kpi_period
          and d.kpi_period_value  = vv_period_value
          and d.run_date          < vv_run_date
      );
      
    /* X�a d�ng d? li?u t?ng h?p th?a:
       Tr??ng h?p: ko khai b�o KPI_QUOTA, ho?c khai b�o KPI_QUOTA.WEIGHTED <= 0*/
    delete RPT_KPI_CYCLE rkce
    where rkce.cycle_id = vv_period_value
      and (
        -- khai bao QUOTA, nhung nvl(weighted, 0) <= 0
        exists (
          select 1
          from KPI_QUOTA kqa
          where rkce.object_id = kqa.object_id
            and ((rkce.object_type = 1 and kqa.object_type  = 2) --NV
              or (rkce.object_type = 3 and kqa.object_type  = 1) --NPP
            )
            and rkce.kpi_register_id = kqa.kpi_register_id
            and rkce.kpi_config_id   = kqa.kpi_config_id
            and kqa.kpi_period_value = rkce.cycle_id
            and not(kqa.weighted is not null and kqa.weighted > 0) -- ko duoc khai bao QUOTA
        )
        -- ko duoc khai bao QUOTA
        or not exists (
          select 1
          from KPI_QUOTA kqa
          where rkce.object_id = kqa.object_id
            and ((rkce.object_type = 1 and kqa.object_type  = 2) --NV
              or (rkce.object_type = 3 and kqa.object_type  = 1) --NPP
            )
            and rkce.kpi_register_id = kqa.kpi_register_id
            and rkce.kpi_config_id   = kqa.kpi_config_id
            and kqa.kpi_period_value = rkce.cycle_id
        )
      );
      
    /*xoa nhung dong tong hop dang ky KPI cho loai NV, 
        nhung ton tai dong dang ky cho NV*/
    delete RPT_KPI_CYCLE rpt
    where rpt.cycle_id = vv_cperiod_value
      and rpt.object_type = 1 -- NV  
      and exists (
        select 1
        from KPI_REGISTER_HIS krr
        where rpt.kpi_register_id = krr.kpi_register_id
          and krr.object_type = 4 -- Loai NV
          -- and krr.kpi_period = 1 and krr.kpi_period_value = rpt.cycle_id
          and krr.run_date >= vv_run_date
      ) -- KPI gan cho loai NV  
      and exists (
        select 1
        from KPI_REGISTER_HIS krr
        where krr.object_id = rpt.object_id 
          and krr.object_type = 2
          and krr.kpi_period = 1 and krr.kpi_period_value = rpt.cycle_id
          and krr.run_date >= vv_run_date
      ) -- nhung ton tai KPI gan cho NV
    ;
    
    -- X�a d�ng ??ng k� th?a.
    delete KPI_REGISTER_HIS d
    where d.kpi_period        = i_kpi_period
      and d.kpi_period_value  = vv_period_value
      and d.run_date          < vv_run_date;
    
    COMMIT;
  end P_KPI_REGISTER_HIS_FCYCLE;

  PROCEDURE P_KPI_AMOUNT_STAFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [doanh s?, s?n l??ng; nh�n vi�n; chu k?];
    @author: thuattq1
    
    
    @params:  
      i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n; 4: lo?i nh�n vi�n.
      i_object_id           : ID nh�n vi�n/lo?i nv.
      i_kpi_period_value    : ID gi� tr? k?.
      i_kpi_group_config_id : ID nh�m KPI.
      i_kpi_config_id       : ID KPI.
      i_kpi_config_code     : M� KPI.
      i_plan_type           : lo?i ph�n b?: 1: ko ph�n b?; 2: c� ph�n b?.
      i_kpi_reg_id          : ID KPI_REGISTER.
      i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_kpi_period    number;
    v_group_column  varchar2(100);
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_params  nvarchar2(2000);
    v_score   RPT_KPI_CYCLE.SCORE%TYPE;
    --v_weighted      number;
    v_atual_column  varchar2(50);
    v_imp_column    varchar2(50);
    v_count_param   number;
    vv_specific_type STAFF_TYPE.specific_type%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_AMOUNT_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.staff_id%type,
      shop_id         SHOP.shop_id%type,
      weighted        KPI_GROUP_DETAIL.weighted%type,
      max_value       KPI_GROUP_DETAIL.max_value%type,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.plan%type,
      gain            RPT_KPI_CYCLE.done%type,
      gain_ir         RPT_KPI_CYCLE.done_ir%type,
      gain_or         RPT_KPI_CYCLE.done_or%type
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type   is null
        or i_object_id  is null
        or v_kpi_period is null
        or i_kpi_period_value     is null
        or i_kpi_group_config_id  is null
        or i_kpi_config_id    is null
        or i_kpi_config_code  is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2; 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [1: NVBH; 2: GSNPP; 3: tren GSNPP]
    if vv_specific_type not in (1, 2, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [1, 2, 3]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    if i_kpi_config_code in (
         'AMOUNT_STAFF_PRODUCT' , 'AMOUNT_STAFF_CAT'    , 'AMOUNT_STAFF_SUBCAT'
       , 'AMOUNT_STAFF_BRAND'   , 'AMOUNT_STAFF_FLAVOUR', 'AMOUNT_STAFF_PACKING'
       , 'AMOUNT_STAFF_UOM'     , 'AMOUNT_STAFF_VOLUMN' , 'AMOUNT_STAFF_ALL'
    ) then
    
      v_atual_column := 'amount';
      v_imp_column := 'amount_approved';
    elsif i_kpi_config_code in (
         'QUANTITY_STAFF_PRODUCT' , 'QUANTITY_STAFF_CAT'    , 'QUANTITY_STAFF_SUBCAT'
       , 'QUANTITY_STAFF_BRAND'   , 'QUANTITY_STAFF_FLAVOUR', 'QUANTITY_STAFF_PACKING'
       , 'QUANTITY_STAFF_UOM'     , 'QUANTITY_STAFF_VOLUMN' , 'QUANTITY_STAFF_ALL'
    ) then
    
      v_atual_column := 'quantity';
      v_imp_column := 'quantity_approved';
    else
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'invalidate kpi config code');
      return;
    end if;
    
    if i_kpi_config_code in ('AMOUNT_STAFF_PRODUCT', 'QUANTITY_STAFF_PRODUCT') then
        v_group_column := 'product_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 2
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              where ptt.status in (0, 1)
                and ptt.product_code in (
                    select kpve.value
                    from KPI_PARAM_VALUE kpve
                    join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                    where kpve.kpi_config_id = ' || i_kpi_config_id || '
                      and kpve.status in (0, 1)
                      and kpm.type = ' || 2 || '
                      and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                      and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null))) ';
        end if;  
    elsif i_kpi_config_code in ('AMOUNT_STAFF_CAT', 'QUANTITY_STAFF_CAT') then 
        v_group_column := 'cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 1
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.cat_id = pioo.product_info_id 
              where ptt.status in (0, 1)
                and pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 1 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null))) ';
        end if;
    elsif i_kpi_config_code in ('AMOUNT_STAFF_SUBCAT', 'QUANTITY_STAFF_SUBCAT') then 
        v_group_column := 'sub_cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 8
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.sub_cat_id = pioo.product_info_id 
              where ptt.status in (0, 1)
                and pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 8 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null))) ';
        end if;
    elsif i_kpi_config_code in ('AMOUNT_STAFF_BRAND', 'QUANTITY_STAFF_BRAND') then 
        v_group_column := 'brand_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 3
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.brand_id = pioo.product_info_id
              where ptt.status in (0, 1)
                and pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 3 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null))) ';
        end if;
    elsif i_kpi_config_code in ('AMOUNT_STAFF_FLAVOUR', 'QUANTITY_STAFF_FLAVOUR') then 
        v_group_column := 'flavour_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 4
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.flavour_id = pioo.product_info_id
              where ptt.status in (0, 1)
                and pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 4 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null))) ';
        end if;
    elsif i_kpi_config_code in ('AMOUNT_STAFF_PACKING', 'QUANTITY_STAFF_PACKING') then 
        v_group_column := 'packing_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 5
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.packing_id = pioo.product_info_id 
              where ptt.status in (0, 1)
                and pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 5 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null))) ';
        end if;
    elsif i_kpi_config_code in ('AMOUNT_STAFF_UOM', 'QUANTITY_STAFF_UOM') then 
        v_group_column := 'uom1';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 7
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              where ptt.status in (0, 1)
                and ptt.uom1 in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 7 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null))) ';
        end if;  
    elsif i_kpi_config_code in ('AMOUNT_STAFF_VOLUMN', 'QUANTITY_STAFF_VOLUMN') then 
        v_group_column := 'volumn';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 6
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              where ptt.status in (0, 1) 
                and to_char(ptt.volumn) in (
                  select replace(kpve.value, ''0.'', ''.'')
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 6 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null))) ';
        end if;
    elsif i_kpi_config_code in ('AMOUNT_STAFF_ALL', 'QUANTITY_STAFF_ALL') then
        -- CHAY FULL
        v_group_column := '-1';
    else
        -- CHAY FULL
        v_group_column := '-1';
    end if;  
    
    if vv_specific_type in (2, 3) then 
      -- GSNPP, tren GSNPP:: vv_specific_type
      v_sql := 
        '-- ds NV tinh KPI
        with sf_tmp as (
          select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
            , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
          from STAFF sf 
          join STAFF_TYPE_TMP ste 
          on ste.staff_id = sf.staff_id
          where sf.status = 1 ' ||
            (case when i_object_type = 2 then 
                    ' and sf.staff_id = ' || i_object_id || ' '
                  when i_object_type = 4 then 
                    ' and ste.staff_type_id = ' || i_object_id || ' '
             end)|| ' 
        )
        -- ds NPP truc thuoc
        , isf_tmp as (
            select distinct sf_tmp.istaff_id, sf_tmp.ishop_id, sp.shop_id
            from sf_tmp
            join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
            on sf_tmp.istaff_id = musp.user_id
              and musp.status in (0, 1)
              and musp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
              and (musp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
              and musp.inherit_shop_spec_type = 1 -- NPP
            join SHOP sp
            on musp.inherit_shop_id = sp.shop_id
              and sp.status = 1
        )
        , sp_tmp as (
            select sff.istaff_id as staff_id
                , pt.product_id, nullif(pt.convfact, 0) as convfact
                , sum(nvl(spn.'||v_atual_column||', 0)) as plan
            from isf_tmp sff
            join SALE_PLAN spn
            on sff.shop_id = spn.object_id
            join PRODUCT pt
              on spn.product_id = pt.product_id
              and pt.status in (0, 1)
            where spn.cycle_id = ' || i_kpi_period_value || '
                and spn.'||v_atual_column||' is not null 
                and spn.object_type = 3
                and spn.type = 3
                and spn.status = 1 '|| 
                (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) ||' '|| 
                (case when trim(v_params) is not null then ' and pt.' || v_params else null end) ||' 
            group by sff.istaff_id
              , pt.product_id, pt.convfact
        )
        , rpt_tmp as (
          select sff.istaff_id as staff_id
              , pt.product_id, nullif(pt.convfact, 0) as convfact 
              , sum(nvl(rptt.' || v_imp_column || ', 0)) gain
              , sum(case when rptt.route_type = 1 then nvl(rptt.' || v_imp_column || ', 0) else 0 end) gain_ir
              , sum(case when rptt.route_type = 0 then nvl(rptt.' || v_imp_column || ', 0) else 0 end) gain_or
          from isf_tmp sff
          join RPT_SALE_PRIMARY_MONTH rptt
          on sff.shop_id = rptt.shop_id
          join PRODUCT pt
          on rptt.product_id = pt.product_id
            and pt.status in (0, 1)
          where rptt.cycle_id = ' || i_kpi_period_value || ' 
            ' ||(case when v_group_column = '-1' then null 
                      else ' and pt.' || v_group_column || ' is not null ' 
                 end)
              ||(case when trim(v_params) is not null then ' and rptt.' || v_params else null end) ||' 
          group by sff.istaff_id
            , pt.product_id, pt.convfact
        )
        select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
            , vkrr.plan_type
            , nvl('|| (case when i_plan_type = 2 then 'kqa.plan_value' 
                            when i_plan_type = 1 then 'spn.plan' 
                            else 'null' end) ||', 0) as plan_value
            , rpt.gain
            , rpt.gain_ir
            , rpt.gain_or
        from KPI_REGISTER_HIS vkrr
        join sf_tmp sf 
        on 1 = 1 ' ||
        (case when i_object_type = 2 then 
                ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
              when i_object_type = 4 then 
                ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                  and sf.istaff_id not in (
                    select krhs.object_id
                    from KPI_REGISTER_HIS krhs
                    where krhs.kpi_period = 1
                      and krhs.kpi_period_value = vkrr.kpi_period_value
                      and krhs.object_type = 2 -- NV
                      and krhs.kpi_group_config_id is not null)'
         end) || ' 
        join (
           select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
           from KPI_QUOTA kqat
           where kqat.kpi_period_value = ' || i_kpi_period_value || '
               and kqat.object_type = 2
               and kqat.status = 1
               and kqat.weighted is not null
               and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
               and kqat.kpi_config_id = ' || i_kpi_config_id || '
        ) kqa on kqa.object_id = sf.istaff_id 
        ' || 
        (case when i_plan_type = 1 then 
          'left join (
            select staff_id
              , ' ||(case when v_atual_column = 'quantity' then 'round(sum(nvl(plan/convfact, 0)), 2)'
                          else 'sum(nvl(plan, 0)) ' end) || ' as plan
            from sp_tmp
            group by staff_id
          ) spn 
          on spn.staff_id = sf.istaff_id '
         end) || ' 
        left join (
          select staff_id
            '|| (case when v_atual_column = 'quantity' then
                         ', round(sum(nvl(gain   /convfact, 0)), 2) gain
                          , round(sum(nvl(gain_ir/convfact, 0)), 2) gain_ir
                          , round(sum(nvl(gain_or/convfact, 0)), 2) gain_or'
                      else 
                         ', sum(nvl(gain   , 0)) gain
                          , sum(nvl(gain_ir, 0)) gain_ir
                          , sum(nvl(gain_or, 0)) gain_or'
                 end)|| '
          from rpt_tmp
          group by staff_id
        ) rpt
        on sf.istaff_id = rpt.staff_id
        where vkrr.kpi_period = 1 -- chu ky
          and vkrr.kpi_period_value = ' || i_kpi_period_value || '
          and vkrr.object_type = ' || i_object_type || '
          and vkrr.kpi_config_id = ' || i_kpi_config_id || '
          and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
          and vkrr.plan_type = ' || i_plan_type      
          /*|| (case when i_plan_type = 2 then 'and kqa.plan_value is not null ' 
                when i_plan_type = 1 then 'and spn.plan is not null ' 
                else null end)*/
      ;
    elsif vv_specific_type = 1 then 
      -- mac dinh NV vv_specific_type = 1
      v_sql := 
        '-- ds NV tinh KPI
        with sf_tmp as (
          select distinct sf.staff_id, sf.shop_id
            , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
          from STAFF sf 
          join STAFF_TYPE_TMP ste 
          on ste.staff_id = sf.staff_id
          where sf.status = 1 ' ||
            (case when i_object_type = 2 then ' and sf.staff_id = ' || i_object_id || ' '
                  when i_object_type = 4 then ' and ste.staff_type_id = ' || i_object_id || ' '
             end) || ' 
        )
        , sp_tmp as (
          select sff.staff_id
              , pt.product_id, nullif(pt.convfact, 0) as convfact
              , sum(nvl(spn.'||v_atual_column||', 0)) as plan
          from sf_tmp sff
          join SALE_PLAN spn
          on sff.staff_id = spn.object_id
          join PRODUCT pt
            on spn.product_id = pt.product_id
            and pt.status in (0, 1)
          where spn.cycle_id = ' || i_kpi_period_value || '
              and spn.'||v_atual_column||' is not null 
              and spn.object_type = 1
              and spn.type = 2
              and exists (
                select 1
                from MAP_USER_SHOP musp
                where sff.staff_id = musp.user_id
                  and musp.inherit_shop_id = spn.shop_id -- chi lay doanh so NPP cuoi cung
                  and musp.status in (0, 1)
                  and musp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                  and (musp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') 
                    or musp.to_date is null)
                  and musp.inherit_shop_spec_type = 1)
              and spn.status = 1 '
              || (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) 
              || (case when trim(v_params) is not null then ' and pt.' || v_params else null end) ||' 
          group by sff.staff_id
            , pt.product_id, pt.convfact
        )
        , rpt_tmp as (
          select sff.staff_id
            , pt.product_id, nullif(pt.convfact, 0) as convfact
            , sum(nvl(rptt.' || v_imp_column || ', 0)) gain
            , sum(case when rptt.route_type = 1 then nvl(rptt.' || v_imp_column || ', 0) else 0 end) gain_ir
            , sum(case when rptt.route_type = 0 then nvl(rptt.' || v_imp_column || ', 0) else 0 end) gain_or
          from sf_tmp sff
          join RPT_SALE_PRIMARY_MONTH rptt
          on sff.staff_id = rptt.staff_id
          join PRODUCT pt
          on rptt.product_id = pt.product_id
            and pt.status in (0, 1)
          where rptt.cycle_id = ' || i_kpi_period_value || ' ' || 
              /*(case when i_plan_type = 1 then '
                and exists (
                    select 1
                    from SALE_PLAN spn
                    where spn.product_id = pt.product_id
                        and rptt.staff_id = spn.object_id
                        and spn.cycle_id = ' || i_kpi_period_value || '
                        and spn.'||v_atual_column||' is not null 
                        and spn.object_type = 1
                        and spn.type = 2
                        and spn.status = 1 '|| 
                        (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) ||' '|| 
                        (case when trim(v_params) is not null then ' and pt.' || v_params else null end) ||' ) '
               end) || ' '||*/ 
              (case when v_group_column = '-1' then null 
                    else ' and pt.' || v_group_column || ' is not null ' 
               end) ||' '|| 
              (case when trim(v_params) is not null then ' and rptt.' || v_params else null end) ||' 
          group by sff.staff_id
            , pt.product_id, pt.convfact
        )
        select sf.staff_id, sf.shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
            , vkrr.plan_type
            , nvl('|| (case when i_plan_type = 2 then 'kqa.plan_value' 
                            when i_plan_type = 1 then 'spn.plan' 
                            else 'null' end)|| ', 0) as plan_value
            , rpt.gain
            , rpt.gain_ir
            , rpt.gain_or
        from KPI_REGISTER_HIS vkrr
        join sf_tmp sf 
        on 1 = 1 ' ||
          (case when i_object_type = 2 then 
                  ' and sf.staff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
                when i_object_type = 4 then 
                  ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                    and sf.staff_id not in (
                      select krhs.object_id
                      from KPI_REGISTER_HIS krhs
                      where krhs.kpi_period = 1
                        and krhs.kpi_period_value = vkrr.kpi_period_value
                        and krhs.object_type = 2 -- NV
                        and krhs.kpi_group_config_id is not null)'
           end) ||' 
        join (
           select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
           from KPI_QUOTA kqat
           where kqat.kpi_period_value = ' || i_kpi_period_value || '
               and kqat.object_type = 2
               and kqat.status = 1
               and kqat.weighted is not null
               and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
               and kqat.kpi_config_id = ' || i_kpi_config_id || '
        ) kqa on kqa.object_id = sf.staff_id
        ' || 
        (case when i_plan_type = 1 then 
           'left join (
              select staff_id
                , ' ||(case when v_atual_column = 'quantity' then
                              'round(sum(nvl(plan/convfact, 0)), 2)'
                       else 'sum(nvl(plan, 0)) ' end) || ' as plan
              from sp_tmp
              group by staff_id
            ) spn 
            on spn.staff_id = sf.staff_id '
         end) ||    
        ' left join (
            select staff_id
              ' ||(case when v_atual_column = 'quantity' then
                         ', round(sum(nvl(gain   /convfact, 0)), 2) gain
                          , round(sum(nvl(gain_ir/convfact, 0)), 2) gain_ir
                          , round(sum(nvl(gain_or/convfact, 0)), 2) gain_or'
                        else 
                         ', sum(nvl(gain   , 0)) gain
                          , sum(nvl(gain_ir, 0)) gain_ir
                          , sum(nvl(gain_or, 0)) gain_or'
                   end)|| '
            from rpt_tmp
            group by staff_id
        ) rpt
        on sf.staff_id = rpt.staff_id
        where vkrr.kpi_period = 1 -- chu ky
          and vkrr.kpi_period_value = ' || i_kpi_period_value || '
          and vkrr.object_type = ' || i_object_type || '
          and vkrr.kpi_config_id = ' || i_kpi_config_id || '
          and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
          and vkrr.plan_type = ' || i_plan_type      
          /*|| (case when i_plan_type = 2 then 'and kqa.plan_value is not null ' 
                when i_plan_type = 1 then 'and spn.plan is not null ' 
                else null end)*/
      ;
    end if;

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   done_ir             = v_dta(indx).gain_ir,
                   done_or             = v_dta(indx).gain_or,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, done_ir, 
               done_or, score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_dta(indx).gain_ir,
             v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, done_ir, 
               done_or, score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_dta(indx).gain_ir,
             v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_AMOUNT_STAFF_CYCLE;

  PROCEDURE P_KPI_ASO_STAFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [?? ph?; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_kpi_period    number;
    v_group_column  varchar2(100);
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_params  nvarchar2(2000);
    v_score   RPT_KPI_CYCLE.SCORE%TYPE;
    v_weighted    number;
    v_count_param number;
    vv_specific_type STAFF_TYPE.specific_type%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_ASO_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type   is null
        or i_object_id  is null
        or v_kpi_period is null
        or i_kpi_period_value is null
        or i_kpi_group_config_id is null
        or i_kpi_config_id is null
        or i_kpi_config_code is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2; 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [1: NVBH; 2: GSNPP; 3: tren GSNPP]
    if vv_specific_type not in (1, 2, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [1, 2, 3]');
      return;
    end if;
    
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    if i_kpi_config_code in ('ASO_STAFF_PRODUCT') then
        v_group_column := 'product_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 2
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where ptt.product_code in (
                    select kpve.value
                    from KPI_PARAM_VALUE kpve
                    join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                    where kpve.kpi_config_id = ' || i_kpi_config_id || '
                      and kpve.status in (0, 1)
                      and kpm.type = ' || 2 || '
                      and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                      and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('ASO_STAFF_CAT') then 
        v_group_column := 'cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 1
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 1 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('ASO_STAFF_SUBCAT') then 
        v_group_column := 'sub_cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 8
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.sub_cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 8 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('ASO_STAFF_BRAND') then 
        v_group_column := 'brand_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 3
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.brand_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 3 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('ASO_STAFF_FLAVOUR') then 
        v_group_column := 'flavour_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 4
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.flavour_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 4 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('ASO_STAFF_PACKING') then 
        v_group_column := 'packing_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 5
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.packing_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 5 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('ASO_STAFF_UOM') then 
        v_group_column := 'uom1';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 7
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where ptt.uom1 in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 7 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('ASO_STAFF_VOLUMN') then 
        v_group_column := 'volumn';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 6
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where to_char(ptt.volumn) in (
                  select replace(kpve.value, ''0.'', ''.'')
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 6 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('ASO_STAFF_ALL') then
        -- CHAY FULL
        v_group_column := '-1';
    else       
        -- CHAY FULL
        v_group_column := '-1';
    end if;  
    
    v_sql := 
      '-- ds NV tinh KPI
      with sf_tmp as (
        select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
          , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
        from STAFF sf 
        join STAFF_TYPE_TMP ste 
        on ste.staff_id = sf.staff_id
        where sf.status = 1 ' ||
          (case when i_object_type = 2 then 
            ' and sf.staff_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
            ' and ste.staff_type_id = ' || i_object_id || ' '
          end)
      || ' )
      -- ds NV truc thuoc
      , isf_tmp as ( ' || 
      (case when vv_specific_type = 2 then -- GSNPP: vv_specific_type
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join MAP_USER_STAFF musf
              on musf.user_id = sf_tmp.istaff_id
                and musf.status in (0, 1)
                and musf.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (musf.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musf.to_date is null)
              join STAFF sf
              on musf.inherit_staff_id = sf.staff_id
                and sf.status = 1
                and exists (
                  select 1
                  from STAFF_TYPE ste 
                  where ste.staff_type_id = sf.staff_type_id
                    and ste.status = 1
                    and ste.specific_type = 1) '
            when vv_specific_type = 3 then -- tren GSNPP: vv_specific_type
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
              on sf_tmp.istaff_id = musp.user_id
                and musp.status in (0, 1)
                and musp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (musp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
                and musp.inherit_shop_spec_type = 1 -- NPP
              join SHOP sp
              on musp.inherit_shop_id = sp.shop_id
                and sp.status = 1
              join MAP_USER_SHOP muspp
              on sp.shop_id = muspp.inherit_shop_id
                and muspp.status in (0, 1)
                and muspp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (muspp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or muspp.to_date is null)
              join STAFF sf
              on muspp.user_id = sf.staff_id 
                and sf.status = 1
                and exists (
                  select 1
                  from STAFF_TYPE ste 
                  where ste.staff_type_id = sf.staff_type_id
                    and ste.status = 1
                    and ste.specific_type = 1) '
            else -- mac dinh NV vv_specific_type = 1
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join STAFF sf
              on sf_tmp.istaff_id = sf.staff_id '
      end)
      || ')
      select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , kqa.plan_value as plan_value
          , round(nvl(rpt.gain, 0) * 100/ nullif(spn.plan, 0), 2) as gain
      from KPI_REGISTER_HIS vkrr
      join sf_tmp sf 
      on 1 = 1 ' ||
      (case when i_object_type = 2 then 
              ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
              ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                and sf.istaff_id not in (
                      select krhs.object_id
                      from KPI_REGISTER_HIS krhs
                      where krhs.kpi_period = 1
                        and krhs.kpi_period_value = vkrr.kpi_period_value
                        and krhs.object_type = 2 -- NV
                        and krhs.kpi_group_config_id is not null)'
       end) ||
      ' join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.status         = 1
              and kqat.weighted is not null
              and kqat.object_type    = 2
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
      join (
        select sff.istaff_id as staff_id, count(distinct cr.customer_id) plan
        from isf_tmp sff
        join VISIT_PLAN vpn
        on vpn.staff_id = sff.staff_id
        join ROUTING rg
        on rg.status = 1
          and rg.shop_id = vpn.shop_id
          and vpn.routing_id = rg.routing_id
        join ROUTING_CUSTOMER rcr
        on rcr.status = 1 
          and rcr.routing_id = rg.routing_id
        join CUSTOMER cr
        on cr.status = 1
          and rcr.customer_id = cr.customer_id
        where vpn.status = 1
          and exists (
            select 1 
            from CYCLE ce
            where ce.cycle_id = ' || i_kpi_period_value || '
              and vpn.from_date <= ce.end_date
              and (vpn.to_date >= ce.begin_date or vpn.to_date is null)
              and rcr.start_date <= ce.end_date
              and (rcr.end_date >= ce.begin_date or rcr.end_date is null)
          )
        group by sff.istaff_id
      ) spn 
      on spn.staff_id = sf.istaff_id
      left join (
        select dta.staff_id
          , count(distinct dta.customer_id) as gain
        from (
            select sff.istaff_id as staff_id, rptt.customer_id
              , sum(nvl(rptt.amount_approved, 0)) as amount
            from isf_tmp sff
            join RPT_SALE_PRIMARY_MONTH rptt
            on sff.staff_id = rptt.staff_id
            where rptt.cycle_id = ' || i_kpi_period_value || '
              and rptt.customer_id is not null
              '|| (case when v_group_column = '-1' then null 
                        when v_group_column = 'volumn' then
                          ' and exists (select 1 from product ptt where ptt.product_id = rptt.product_id and ptt.' || v_group_column || ' is not null) '
                        else ' and rptt.' || v_group_column || ' is not null ' end) ||'
              '|| (case when trim(v_params) is not null then ' and rptt.' || v_params else null end) ||'
            group by sff.istaff_id, rptt.customer_id
            having sum(nvl(rptt.amount_approved, 0)) > 0
        ) dta
        where dta.amount > 0
        group by dta.staff_id
      ) rpt
      on sf.istaff_id = rpt.staff_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type || '
        and kqa.plan_value is not null ';

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          -- t�nh ?i?m cho t?ng ti�u ch� KPI trong b?.
          /*if v_dta(indx).plan_value is null 
              or (v_dta(indx).plan_value <= 0 and v_dta(indx).gain is null )
          then
            v_score := 0;
          elsif v_dta(indx).plan_value <= 0 then
              v_score := round(((1 * nvl(v_dta(indx).weighted, 100))/ 100), 2);
          else 
            if i_max_value is not null and v_dta(indx).gain > i_max_value then
              v_score := round((((i_max_value / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            else 
              v_score := round((((v_dta(indx).gain / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            end if;
          end if;*/
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_ASO_STAFF_CYCLE;

  PROCEDURE P_KPI_AVG_AMOUNT_STAFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [trung b�nh doanh s?, s?n l??ng ??n [ch?t l??ng ??n: doanh s?; s?n l??ng]; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
      i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n; 4: lo?i nh�n vi�n.
      i_object_id           : ID nh�n vi�n/lo?i nv.
      i_kpi_period_value    : ID gi� tr? k?.
      i_kpi_group_config_id : ID nh�m KPI.
      i_kpi_config_id       : ID KPI.
      i_kpi_config_code     : M� KPI.
      i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
      i_kpi_reg_id          : ID KPI_REGISTER.
      i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_kpi_period    number;
    v_group_column  varchar2(100);
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg nvarchar2(2000);
    v_params    nvarchar2(2000);
    v_score     RPT_KPI_CYCLE.SCORE%TYPE;
    v_weighted  number;
    v_atual_column  varchar2(50);
    v_imp_column    varchar2(50);
    v_cycle_begin_date cycle.begin_date%type;
    v_count_param     number;
    vv_specific_type  STAFF_TYPE.specific_type%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_AVG_AMOUNT_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE,
      gain_ir         RPT_KPI_CYCLE.DONE_IR%TYPE,
      gain_or         RPT_KPI_CYCLE.DONE_OR%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type is null
        or i_object_id is null
        or v_kpi_period is null
        or i_kpi_period_value is null
        or i_kpi_group_config_id is null
        or i_kpi_config_id is null
        or i_kpi_config_code is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    -- 
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2; 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [1: NVBH; 2: GSNPP; 3: tren GSNPP]
    if vv_specific_type not in (1, 2, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [1, 2, 3]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    begin
      select trunc(begin_date)
      into v_cycle_begin_date
      from cycle ce
      where ce.cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'can''t find cycle with cycle_id = ' || i_kpi_period_value);
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle info with cycle_id =  ' || i_kpi_period_value 
          || '. Exception: ' || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000));
      return;
    end;
    
    if i_kpi_config_code in (
         'AVG_AMOUNT_STAFF_PRODUCT' , 'AVG_AMOUNT_STAFF_CAT'    , 'AVG_AMOUNT_STAFF_SUBCAT'
       , 'AVG_AMOUNT_STAFF_BRAND'   , 'AVG_AMOUNT_STAFF_FLAVOUR', 'AVG_AMOUNT_STAFF_PACKING'
       , 'AVG_AMOUNT_STAFF_UOM'     , 'AVG_AMOUNT_STAFF_VOLUMN' , 'AVG_AMOUNT_STAFF_ALL'
    ) then
    
      v_atual_column := 'amount';
      v_imp_column := 'amount_approved';
    elsif i_kpi_config_code in (
         'AVG_QUANTITY_STAFF_PRODUCT' , 'AVG_QUANTITY_STAFF_CAT'    , 'AVG_QUANTITY_STAFF_SUBCAT'
       , 'AVG_QUANTITY_STAFF_BRAND'   , 'AVG_QUANTITY_STAFF_FLAVOUR', 'AVG_QUANTITY_STAFF_PACKING'
       , 'AVG_QUANTITY_STAFF_UOM'     , 'AVG_QUANTITY_STAFF_VOLUMN' , 'AVG_QUANTITY_STAFF_ALL'
    ) then
    
      v_atual_column := 'quantity';
      v_imp_column := 'quantity_approved';
    else
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'invalidate kpi config code');
      return;
    end if;
    
    if i_kpi_config_code in ('AVG_AMOUNT_STAFF_PRODUCT', 'AVG_QUANTITY_STAFF_PRODUCT') then
        v_group_column := 'product_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 2
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              where ptt.product_code in (
                    select kpve.value
                    from KPI_PARAM_VALUE kpve
                    join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                    where kpve.kpi_config_id = ' || i_kpi_config_id || '
                      and kpve.status in (0, 1)
                      and kpm.type = ' || 2 || '
                      and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                      and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_AMOUNT_STAFF_CAT', 'AVG_QUANTITY_STAFF_CAT') then 
        v_group_column := 'cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 1
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 1 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_AMOUNT_STAFF_SUBCAT', 'AVG_QUANTITY_STAFF_SUBCAT') then 
        v_group_column := 'sub_cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 8
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.sub_cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 8 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_AMOUNT_STAFF_BRAND', 'AVG_QUANTITY_STAFF_BRAND') then 
        v_group_column := 'brand_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 3
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.brand_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 3 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_AMOUNT_STAFF_FLAVOUR', 'AVG_QUANTITY_STAFF_FLAVOUR') then 
        v_group_column := 'flavour_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 4
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.flavour_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 4 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_AMOUNT_STAFF_PACKING', 'AVG_QUANTITY_STAFF_PACKING') then 
        v_group_column := 'packing_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 5
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.packing_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 5 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_AMOUNT_STAFF_UOM', 'AVG_QUANTITY_STAFF_UOM') then 
        v_group_column := 'uom1';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 7
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              where ptt.uom1 in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 7 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_AMOUNT_STAFF_VOLUMN', 'AVG_QUANTITY_STAFF_VOLUMN') then 
        v_group_column := 'volumn';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 6
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              where to_char(ptt.volumn) in (
                  select replace(kpve.value, ''0.'', ''.'')
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 6 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_AMOUNT_STAFF_ALL', 'AVG_QUANTITY_STAFF_ALL') then
        -- CHAY FULL
        v_group_column := '-1';
    else       
        -- CHAY FULL
        v_group_column := '-1';
    end if;  
    
    v_sql := 
      '-- ds NV tinh KPI
      with sf_tmp as (
        select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
          , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
        from STAFF sf 
        join STAFF_TYPE_TMP ste 
        on ste.staff_id = sf.staff_id
        where sf.status = 1 ' ||
          (case when i_object_type = 2 then 
                  ' and sf.staff_id = ' || i_object_id || ' '
                when i_object_type = 4 then 
                  ' and ste.staff_type_id = ' || i_object_id || ' ' 
           end)|| ' 
      )
      -- ds NV truc thuoc
      , isf_tmp as ( ' || 
      (case when vv_specific_type = 2 then -- GSNPP: vv_specific_type
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join MAP_USER_STAFF musf
              on musf.user_id = sf_tmp.istaff_id
                and musf.status in (0, 1)
                and musf.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (musf.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musf.to_date is null)
              join STAFF sf
              on musf.inherit_staff_id = sf.staff_id
                and sf.status = 1
                and exists (
                  select 1
                  from STAFF_TYPE ste 
                  where ste.staff_type_id = sf.staff_type_id
                    and ste.status = 1
                    and ste.specific_type = 1) '
            when vv_specific_type = 3 then -- tren GSNPP: vv_specific_type
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
              on sf_tmp.istaff_id = musp.user_id
                and musp.status in (0, 1)
                and musp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (musp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
                and musp.inherit_shop_spec_type = 1 -- NPP
              join SHOP sp
              on musp.inherit_shop_id = sp.shop_id
                and sp.status = 1
              join MAP_USER_SHOP muspp
              on sp.shop_id = muspp.inherit_shop_id
                and muspp.status in (0, 1)
                and muspp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (muspp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or muspp.to_date is null)
              join STAFF sf
              on muspp.user_id = sf.staff_id 
                and sf.status = 1
                and exists (
                  select 1
                  from STAFF_TYPE ste 
                  where ste.staff_type_id = sf.staff_type_id
                    and ste.status = 1
                    and ste.specific_type = 1) '
            else -- mac dinh NV vv_specific_type = 1
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join STAFF sf
              on sf_tmp.istaff_id = sf.staff_id '
      end)
      || ')
      select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , kqa.plan_value as plan_value' ||
          (case when v_atual_column = 'quantity' then 
                 ', round(rpt.gain    / nullif(sor.total_order   , 0), 2) as gain
                  , round(rpt.gain_ir / nullif(sor.total_order_ir, 0), 2) as gain_ir
                  , round(rpt.gain_or / nullif(sor.total_order_or, 0), 2) as gain_or'
                else
                 ', round(rpt.gain    / nullif(sor.total_order   , 0)) as gain
                  , round(rpt.gain_ir / nullif(sor.total_order_ir, 0)) as gain_ir
                  , round(rpt.gain_or / nullif(sor.total_order_or, 0)) as gain_or'
           end) || '
      from KPI_REGISTER_HIS vkrr
      join sf_tmp sf 
      on 1 = 1 ' ||
        (case when i_object_type = 2 then 
                ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
              when i_object_type = 4 then 
                ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                  and sf.istaff_id not in (
                      select krhs.object_id
                      from KPI_REGISTER_HIS krhs
                      where krhs.kpi_period = 1
                        and krhs.kpi_period_value = vkrr.kpi_period_value
                        and krhs.object_type = 2 -- NV
                        and krhs.kpi_group_config_id is not null)'
         end) || ' 
      join (
        select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
        from KPI_QUOTA kqat
        where kqat.kpi_period_value = ' || i_kpi_period_value || '
            and kqat.status         = 1
            and kqat.weighted is not null
            and kqat.object_type    = 2
            and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
            and kqat.kpi_config_id  = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
      left join (
        select sff.istaff_id as staff_id
          , count(distinct sor.sale_order_id) as total_order
          , count(distinct case when sor.is_visit_plan = 1 then sor.sale_order_id else null end) as total_order_ir
          , count(distinct case when sor.is_visit_plan = 0 then sor.sale_order_id else null end) as total_order_or
        from isf_tmp sff
        join SALE_ORDER sor
        on sor.staff_id = sff.staff_id
        join SALE_ORDER_DETAIL sodl
          on sor.sale_order_id = sodl.sale_order_id
        join PRODUCT pt
          on sodl.product_id = pt.product_id
        where sor.cycle_id  = ' || i_kpi_period_value || '
          and sor.approved  in (1)
          and sor.type      = 1 
          and sor.amount    > 0 -- lay don co doanh so
          and sodl.order_date >= to_date(''' || to_char(v_cycle_begin_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'')
          and sor.order_date >= to_date(''' || to_char(v_cycle_begin_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') ' || 
          (case when v_group_column = '-1' then null 
                else ' and pt.' || v_group_column || ' is not null ' end) ||
          (case when trim(v_params) is not null then ' and pt.' || v_params 
                else null end) ||'
        group by sff.istaff_id
      ) sor on sor.staff_id = sf.istaff_id
      left join (
        select sff.istaff_id as staff_id ' ||
          (case when v_imp_column = 'quantity_approved' then
                 ', sum(nvl(rptt.quantity_approved/nullif(pt.convfact, 0), 0)) gain
                  , sum(case when rptt.route_type = 1 then nvl(rptt.quantity_approved/nullif(pt.convfact, 0), 0) else 0 end) gain_ir
                  , sum(case when rptt.route_type = 0 then nvl(rptt.quantity_approved/nullif(pt.convfact, 0), 0) else 0 end) gain_or'
                else
                 ', sum(nvl(rptt.' || v_imp_column || ', 0)) gain
                  , sum(case when rptt.route_type = 1 then nvl(rptt.' || v_imp_column || ', 0) else 0 end) gain_ir
                  , sum(case when rptt.route_type = 0 then nvl(rptt.' || v_imp_column || ', 0) else 0 end) gain_or '
           end) ||'
        from isf_tmp sff
        join RPT_SALE_PRIMARY_MONTH rptt
        on sff.staff_id = rptt.staff_id
        join PRODUCT pt
        on rptt.product_id = pt.product_id
        where rptt.cycle_id = ' || i_kpi_period_value || 
          (case when v_group_column = '-1' then null 
                when v_group_column = 'volumn' then
                  ' and exists (select 1 from product ptt where ptt.product_id = rptt.product_id and ptt.' || v_group_column || ' is not null) '
                else ' and rptt.' || v_group_column || ' is not null ' end) ||
          (case when trim(v_params) is not null then ' and rptt.' || v_params 
                else null end) ||' 
        group by sff.istaff_id
      ) rpt
      on sf.istaff_id = rpt.staff_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type || '
        and kqa.plan_value is not null ';

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          -- t�nh ?i?m cho t?ng ti�u ch� KPI trong b?.
          /*if v_dta(indx).plan_value is null 
              or (v_dta(indx).plan_value <= 0 and v_dta(indx).gain is null )
          then
            v_score := 0;
          elsif v_dta(indx).plan_value <= 0 then
              v_score := round(((1 * nvl(v_dta(indx).weighted, 100))/ 100), 2);
          else 
            if i_max_value is not null and v_dta(indx).gain > i_max_value then
              v_score := round((((i_max_value / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            else 
              v_score := round((((v_dta(indx).gain / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            end if;
          end if;*/
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   done_ir             = v_dta(indx).gain_ir,
                   done_or             = v_dta(indx).gain_or,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
              and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id, 
               plan, done, done_ir, 
               done_or, score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_dta(indx).gain_ir,
             v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id, 
               plan, done, done_ir, 
               done_or, score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_dta(indx).gain_ir,
             v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_AVG_AMOUNT_STAFF_CYCLE;

  PROCEDURE P_KPI_AVG_ORDER_STAFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [trung b�nh ??n h�ng th�nh c�ng; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_kpi_period    number;
    v_group_column  varchar2(100);
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_params  nvarchar2(2000);
    v_score   RPT_KPI_CYCLE.SCORE%TYPE;
    v_weighted      number;
    vv_bcycle_date  cycle.begin_date%type;
    vv_ecycle_date  cycle.end_date%type;
    v_count_param   number;
    vv_specific_type STAFF_TYPE.specific_type%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_AVG_ORDER_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE,
      gain_ir         RPT_KPI_CYCLE.DONE_IR%TYPE,
      gain_or         RPT_KPI_CYCLE.DONE_OR%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type is null
      or i_object_id  is null
      or v_kpi_period is null
      or i_kpi_period_value     is null
      or i_kpi_group_config_id  is null
      or i_kpi_config_id    is null
      or i_kpi_config_code  is null
      or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    -- 
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2; 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [1: NVBH; 2: GSNPP; 3: tren GSNPP]
    if vv_specific_type not in (1, 2, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [1, 2, 3]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    begin
      select trunc(begin_date), trunc(end_date)
      into vv_bcycle_date, vv_ecycle_date
      from cycle ce
      where ce.cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'can''t find cycle with cycle_id = ' || i_kpi_period_value);
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle info with cycle_id =  ' || i_kpi_period_value 
          || '. Exception: ' || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000));
      return;
    end;
    
    if i_kpi_config_code in ('AVG_ORDER_STAFF_PRODUCT') then
        v_group_column := 'product_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 2
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where ptt.product_code in (
                    select kpve.value
                    from KPI_PARAM_VALUE kpve
                    join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                    where kpve.kpi_config_id = ' || i_kpi_config_id || '
                      and kpve.status in (0, 1)
                      and kpm.type = ' || 2 || '
                      and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                      and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_ORDER_STAFF_CAT') then 
        v_group_column := 'cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 1
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 1 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_ORDER_STAFF_SUBCAT') then 
        v_group_column := 'sub_cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 8
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.sub_cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 8 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_ORDER_STAFF_BRAND') then 
        v_group_column := 'brand_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 3
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.brand_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 3 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_ORDER_STAFF_FLAVOUR') then 
        v_group_column := 'flavour_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 4
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.flavour_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 4 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_ORDER_STAFF_PACKING') then 
        v_group_column := 'packing_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 5
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.packing_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 5 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_ORDER_STAFF_UOM') then 
        v_group_column := 'uom1';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 7
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where ptt.uom1 in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 7 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_ORDER_STAFF_VOLUMN') then 
        v_group_column := 'volumn';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 6
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where to_char(ptt.volumn) in (
                  select replace(kpve.value, ''0.'', ''.'')
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 6 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AVG_ORDER_STAFF_ALL') then
        -- CHAY FULL
        v_group_column := '-1';
    else       
        -- CHAY FULL
        v_group_column := '-1';
    end if;  
    
    v_sql := 
      '-- ds NV tinh KPI
      with sf_tmp as (
        select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
          , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
        from STAFF sf 
        join STAFF_TYPE_TMP ste 
        on ste.staff_id = sf.staff_id
        where sf.status = 1 ' ||
          (case when i_object_type = 2 then 
            ' and sf.staff_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
            ' and ste.staff_type_id = ' || i_object_id || ' '
          end)
      || ' )
      -- ds NV truc thuoc
      , isf_tmp as ( ' || 
      (case when vv_specific_type = 2 then -- GSNPP: vv_specific_type
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join MAP_USER_STAFF musf
              on musf.user_id = sf_tmp.istaff_id
                and musf.status in (0, 1)
                and musf.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (musf.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musf.to_date is null)
              join STAFF sf
              on musf.inherit_staff_id = sf.staff_id
                and sf.status = 1
                and exists (
                  select 1
                  from STAFF_TYPE ste 
                  where ste.staff_type_id = sf.staff_type_id
                    and ste.status = 1
                    and ste.specific_type = 1) '
            when vv_specific_type = 3 then -- tren GSNPP: vv_specific_type
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
              on sf_tmp.istaff_id = musp.user_id
                and musp.status in (0, 1)
                and musp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (musp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
                and musp.inherit_shop_spec_type = 1 -- NPP
              join SHOP sp
              on musp.inherit_shop_id = sp.shop_id
                and sp.status = 1
              join MAP_USER_SHOP muspp
              on sp.shop_id = muspp.inherit_shop_id
                and muspp.status in (0, 1)
                and muspp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (muspp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or muspp.to_date is null)
              join STAFF sf
              on muspp.user_id = sf.staff_id 
                and sf.status = 1
                and exists (
                  select 1
                  from STAFF_TYPE ste 
                  where ste.staff_type_id = sf.staff_type_id
                    and ste.status = 1
                    and ste.specific_type = 1) '
            else -- mac dinh NV vv_specific_type = 1
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join STAFF sf
              on sf_tmp.istaff_id = sf.staff_id '
      end)
      || ')
      select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted
          , vkrr.max_value, vkrr.plan_type
          , kqa.plan_value as plan_value
          , round(rpt.total_order    * 100 / nullif(n_visit.num_visit, 0), 2) as gain
          , round(rpt.total_order_ir * 100 / nullif(n_visit.num_visit, 0), 2) as gain_ir
          , round(rpt.total_order_or * 100 / nullif(n_visit.num_visit, 0), 2) as gain_or
      from KPI_REGISTER_HIS vkrr
      join sf_tmp sf 
      on 1 = 1 ' ||
      (case when i_object_type = 2 then 
              ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
              ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                and sf.istaff_id not in (
                      select krhs.object_id
                      from KPI_REGISTER_HIS krhs
                      where krhs.kpi_period = 1
                        and krhs.kpi_period_value = vkrr.kpi_period_value
                        and krhs.object_type = 2 -- NV
                        and krhs.kpi_group_config_id is not null)'
       end) ||
      ' join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value   = ' || i_kpi_period_value || '
              and kqat.object_type      = 2
              and kqat.status           = 1
              and kqat.weighted is not null
              and kqat.kpi_register_id  = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id    = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
      left join (
        select sff.istaff_id as staff_id
            -- , count(distinct trunc(sor.order_date)) as total_date
            , count(distinct (case when sor.type = 1 then sor.sale_order_id else null end)) as total_order
            , count(distinct (case when sor.type = 1 and sor.is_visit_plan = 1 then sor.sale_order_id else null end)) as total_order_ir
            , count(distinct (case when sor.type = 1 and sor.is_visit_plan = 0 then sor.sale_order_id else null end)) as total_order_or
        from isf_tmp sff
        join SALE_ORDER sor
        on sor.staff_id = sff.staff_id
        join SALE_ORDER_DETAIL sodl
          on sor.sale_order_id = sodl.sale_order_id
        join PRODUCT pt
          on sodl.product_id = pt.product_id
        where sor.cycle_id  = ' || i_kpi_period_value || '
          and sor.approved  in (1)
          and sor.type      = 1 
          and sor.amount    > 0 -- lay don co doanh so
          and sodl.is_free_item = 0
          and sodl.order_date  < to_date(''' || to_char(vv_ecycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
          and sor.order_date   < to_date(''' || to_char(vv_ecycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
          and sodl.order_date >= to_date(''' || to_char(vv_bcycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'')
          and sor.order_date  >= to_date(''' || to_char(vv_bcycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') ' || 
          (case when v_group_column = '-1' then null 
                else ' and pt.' || v_group_column || ' is not null ' end) ||
          (case when trim(v_params) is not null then ' and pt.' || v_params 
                else null end) ||'
        group by sff.istaff_id
      ) rpt
      on sf.istaff_id = rpt.staff_id
      left join (
        select istaff_id as staff_id, count(1) as num_visit
        from (
          select sff.istaff_id, sff.staff_id, alg.customer_id, trunc(alg.start_time)
          from isf_tmp sff
          join ACTION_LOG alg
          on sff.staff_id = alg.staff_id
          where alg.object_type in (0, 1)
            and alg.start_time >= to_date(''' || to_char(vv_bcycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'')
            and alg.start_time  < to_date(''' || to_char(vv_ecycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
            and exists (
              select 1
              from CUSTOMER cr
              where cr.customer_id = alg.customer_id
                and cr.status = 1)
          group by sff.istaff_id, sff.staff_id, alg.customer_id, trunc(alg.start_time)
        )
        group by istaff_id
      ) n_visit
      on sf.istaff_id = n_visit.staff_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type || '
        and kqa.plan_value is not null ';

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          -- t�nh ?i?m cho t?ng ti�u ch� KPI trong b?.
          /*if v_dta(indx).plan_value is null 
              or (v_dta(indx).plan_value <= 0 and v_dta(indx).gain is null )
          then
            v_score := 0;
          elsif v_dta(indx).plan_value <= 0 then
              v_score := round(((1 * nvl(v_dta(indx).weighted, 100))/ 100), 2);
          else 
            if i_max_value is not null and v_dta(indx).gain > i_max_value then
              v_score := round((((i_max_value / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            else 
              v_score := round((((v_dta(indx).gain / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            end if;
          end if;*/
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   done_ir             = v_dta(indx).gain_ir,
                   done_or             = v_dta(indx).gain_or,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, done_ir, 
               done_or, score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_dta(indx).gain_ir,
             v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, done_ir, 
               done_or, score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_dta(indx).gain_ir,
             v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_AVG_ORDER_STAFF_CYCLE;

  PROCEDURE P_KPI_BUY_SHOP_CYCLE (
    i_object_type number,
    i_object_id number,
    i_kpi_period_value number,
    i_kpi_group_config_id number,
    i_kpi_config_id number,
    i_kpi_config_code varchar2,
    i_plan_type number,
    i_kpi_reg_id number,
    i_max_value number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [doanh s?, s?n l??ng nh?p; npp; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 1: NPP c? th?.
    i_object_id           : ID NPP.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 1: ko ph�n b? (l?y t? SALE_PLAN); 2: c� ph�n b? (KPI_QUOTA).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_kpi_period number;
    v_group_column varchar2(100);
    v_rpt_id number(20);
    v_error_type number(2);
    v_error_msg nvarchar2(2000);
    v_params nvarchar2(2000);
    v_score RPT_KPI_CYCLE.SCORE%TYPE;
    v_weighted number;
    v_atual_column  varchar2(50);
    v_count_param   number;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_BUY_SHOP_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE,
      gain_ir         RPT_KPI_CYCLE.DONE_IR%TYPE,
      gain_or         RPT_KPI_CYCLE.DONE_OR%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type is null
        or i_object_id is null
        or v_kpi_period is null
        or i_kpi_period_value is null
        or i_kpi_group_config_id is null
        or i_kpi_config_id is null
        or i_kpi_config_code is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    if i_object_type not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [1]');
      return;
    end if;
    
    -- if v_kpi_period not in (1) then
    --   insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
    --   return;
    -- end if;
    
    if i_kpi_config_code in (
           'AMOUNT_SHOP_PRODUCT'
         , 'AMOUNT_SHOP_CAT'
         , 'AMOUNT_SHOP_SUBCAT'
         , 'AMOUNT_SHOP_BRAND'
         , 'AMOUNT_SHOP_FLAVOUR'
         , 'AMOUNT_SHOP_PACKING'
         , 'AMOUNT_SHOP_UOM'
         , 'AMOUNT_SHOP_VOLUMN'
         , 'AMOUNT_SHOP_ALL') then
      v_atual_column := 'amount';
      -- v_imp_column := 'amount_approved';
    elsif i_kpi_config_code in (
           'QUANTITY_SHOP_PRODUCT'
         , 'QUANTITY_SHOP_CAT'
         , 'QUANTITY_SHOP_SUBCAT'
         , 'QUANTITY_SHOP_BRAND'
         , 'QUANTITY_SHOP_FLAVOUR'
         , 'QUANTITY_SHOP_PACKING'
         , 'QUANTITY_SHOP_UOM'
         , 'QUANTITY_SHOP_VOLUMN'
         , 'QUANTITY_SHOP_ALL') then
      v_atual_column := 'quantity';
      -- v_imp_column := 'quantity_approved';
    else
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'invalidate kpi config code');
      return;
    end if;
    
    if i_kpi_config_code in ('AMOUNT_SHOP_PRODUCT', 'QUANTITY_SHOP_PRODUCT') then
        v_group_column := 'product_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 2
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where ptt.product_code in (
                    select kpve.value
                    from KPI_PARAM_VALUE kpve
                    join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                    where kpve.kpi_config_id = ' || i_kpi_config_id || '
                      and kpve.status in (0, 1)
                      and kpm.type = ' || 2 || '
                      and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                      and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;  
    elsif i_kpi_config_code in ('AMOUNT_SHOP_CAT', 'QUANTITY_SHOP_CAT') then 
        v_group_column := 'cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 1
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 1 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AMOUNT_SHOP_SUBCAT', 'QUANTITY_SHOP_SUBCAT') then 
        v_group_column := 'sub_cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 8
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.sub_cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 8 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AMOUNT_SHOP_BRAND', 'QUANTITY_SHOP_BRAND') then 
        v_group_column := 'brand_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 3
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.brand_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 3 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AMOUNT_SHOP_FLAVOUR', 'QUANTITY_SHOP_FLAVOUR') then 
        v_group_column := 'flavour_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 4
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.flavour_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 4 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AMOUNT_SHOP_PACKING', 'QUANTITY_SHOP_PACKING') then 
        v_group_column := 'packing_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 5
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.packing_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 5 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AMOUNT_SHOP_UOM', 'QUANTITY_SHOP_UOM') then 
        v_group_column := 'uom1';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 7
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where ptt.uom1 in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 7 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;  
    elsif i_kpi_config_code in ('AMOUNT_SHOP_VOLUMN', 'QUANTITY_SHOP_VOLUMN') then 
        v_group_column := 'volumn';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 6
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where to_char(ptt.volumn) in (
                  select replace(kpve.value, ''0.'', ''.'')
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 6 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('AMOUNT_SHOP_ALL', 'QUANTITY_SHOP_ALL') then
        -- CHAY FULL
        v_group_column := '-1';
    else
        -- CHAY FULL
        v_group_column := '-1';
    end if;  
    
    v_sql := 
      'select sp.shop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          '||(case when i_plan_type = 2 then ', kqa.plan_value ' 
                   when i_plan_type = 1 then ', spn.plan ' 
                   else ', null ' end)||' as plan_value
          , rpt.gain
          , rpt.gain_ir
          , rpt.gain_or
      from KPI_REGISTER_HIS vkrr
      join SHOP sp 
      on sp.status = 1 ' ||
      (case when i_object_type = 1 then 
        ' and sp.shop_id = vkrr.object_id and sp.shop_id = ' || i_object_id || ' '
        when i_object_type = 4 then 
        ' and exists (
            select 1
            from SHOP_TYPE ste 
            where ste.shop_type_id = sp.shop_type_id
              and ste.status = 1 
              and ste.shop_type_id = shop.object_id
              and ste.shop_type_id = ' || i_object_id || '
          )'
      end) ||
      (case when i_plan_type = 1 then '
        left join (
            select spn.object_id
                , sum(nvl(spn.' || v_atual_column || ', 0)) plan
            from SALE_PLAN spn
            join product pt
              on spn.product_id = pt.product_id
            where spn.cycle_id = ' || i_kpi_period_value || '
                and spn.object_type = 3
                and spn.type = 2
                '|| (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) ||'
                '|| (case when trim(v_params) is not null then ' and pt.' || v_params else null end) ||' 
            group by spn.object_id
        ) spn 
        on spn.object_id = sp.shop_id 
        '
      end) ||    
      ' join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.status         = 1
              and kqat.weighted is not null
              and kqat.object_type    = 1
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sp.shop_id and kqa.kpi_config_id = vkrr.kpi_config_id
      left join (
        select rptt.shop_id as shop_id
          , sum(nvl(rptt.' || v_atual_column || ', 0)) gain
          , null as gain_ir
          , null as gain_or
        from RPT_BUY_PRIMARY_CYCLE rptt
        where rptt.cycle_id = ' || i_kpi_period_value || '
         ' || (case when i_plan_type = 1 then '
                and exists (
                    select pt.product_id
                    from SALE_PLAN spn
                    join product pt
                      on spn.product_id = pt.product_id
                    where rptt.product_id = pt.product_id
                        and rptt.shop_id = spn.object_id
                        and spn.cycle_id = ' || i_kpi_period_value || '
                        and spn.object_type = 3
                        and spn.type = 2
                        '|| (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) ||'
                        '|| (case when trim(v_params) is not null then ' and pt.' || v_params else null end) ||'
                ) '
              end) || '
          '|| (case when v_group_column = '-1' then null 
                    when v_group_column = 'volumn' then
                      ' and exists (select 1 from product ptt where ptt.product_id = rptt.product_id and ptt.' || v_group_column || ' is not null) '
                    else ' and rptt.' || v_group_column || ' is not null ' end) ||'
          '|| (case when trim(v_params) is not null then ' and rptt.' || v_params else null end) ||' 
        group by rptt.shop_id
      ) rpt
      on sp.shop_id = rpt.shop_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type
        /*|| (case when i_plan_type = 2 then 'and kqa.plan_value is not null ' 
              when i_plan_type = 1 then 'and spn.plan is not null ' 
              else null end)*/
    ;

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 3 -- npp
              and rpt.object_id = v_dta(indx).shop_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          -- t�nh ?i?m cho t?ng ti�u ch� KPI trong b?.
          /*if v_dta(indx).plan_value is null 
              or (v_dta(indx).plan_value <= 0 and v_dta(indx).gain is null )
          then
            v_score := 0;
          elsif v_dta(indx).plan_value <= 0 then
              v_score := round(((1 * nvl(v_dta(indx).weighted, 100))/ 100), 2);
          else 
            if i_max_value is not null and v_dta(indx).gain > i_max_value then
              v_score := round((((i_max_value / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            else 
              v_score := round((((v_dta(indx).gain / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            end if;
          end if;*/
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   -- done_ir             = v_dta(indx).gain_ir,
                   -- done_or             = v_dta(indx).gain_or,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               -- done_ir, done_or, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             3,
             v_dta(indx).shop_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             -- v_dta(indx).gain_ir,
             -- v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               -- done_ir, done_or, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             3,
             v_dta(indx).shop_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             -- v_dta(indx).gain_ir,
             -- v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;shop_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).shop_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_BUY_SHOP_CYCLE;

  PROCEDURE P_KPI_CUS_PASS_KS_STAFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [S? l??ng ?i?m b�n tr?ng b�y ??t chu?n; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n c? th?; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql         clob; 
    v_kpi_period  number;
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_score       RPT_KPI_CYCLE.SCORE%TYPE;
    v_ks_id       KS.KS_ID%TYPE;
    v_cyc_bdate   date;
    v_cyc_edate   date;
    vv_specific_type STAFF_TYPE.specific_type%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_CUS_PASS_KS_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if    i_object_type           is null
      or  i_object_id             is null
      or  v_kpi_period            is null
      or  i_kpi_period_value      is null
      or  i_kpi_group_config_id   is null
      or  i_kpi_config_id         is null
      or  i_kpi_config_code       is null
      or  i_plan_type             is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    begin
      select ce.begin_date, ce.end_date
      into v_cyc_bdate, v_cyc_edate
      from CYCLE ce
      where cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'cycle id not found');
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle|exception: ' 
        || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 200));
      return;
    end;
    
    if v_cyc_bdate is null or v_cyc_edate is null then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'begin date or end date of cycle is null');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2, 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [1: NVBH; 2: GSNPP; 3: tren GSNPP]
    if vv_specific_type not in (1, 2, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [1, 2, 3]');
      return;
    end if;
    
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    -- l?y th�ng tin keyshop
    begin
      select to_number(kpve.value)
      into v_ks_id
      from KPI_PARAM_VALUE kpve
      join KPI_PARAM kpm 
      on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
      where kpve.kpi_config_id = i_kpi_config_id
        and kpve.status in (0, 1)
        and kpm.type = 10
        and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
        and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'not found ks_id');
      return;
    when too_many_rows then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'too many ks_id');
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get ks_id|exception: ' 
          || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 500));
      return;
    end;
    
    v_sql := 
        '-- ds NV tinh KPI
        with sf_tmp as (
          select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
            , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
          from STAFF sf 
          join STAFF_TYPE_TMP ste 
          on ste.staff_id = sf.staff_id
          where sf.status = 1 ' ||
            (case when i_object_type = 2 then 
              ' and sf.staff_id = ' || i_object_id || ' '
              when i_object_type = 4 then 
              ' and ste.staff_type_id = ' || i_object_id || ' '
            end)
        || ' )
        -- ds NV truc thuoc
        , isf_tmp as ( ' || 
        (case when vv_specific_type = 2 then -- GSNPP: vv_specific_type
                ' select distinct sf_tmp.istaff_id, sf.staff_id
                from sf_tmp
                join MAP_USER_STAFF musf
                on musf.user_id = sf_tmp.istaff_id
                  and musf.status in (0, 1)
                  and musf.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                  and (musf.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musf.to_date is null)
                join STAFF sf
                on musf.inherit_staff_id = sf.staff_id
                  and sf.status = 1
                  and exists (
                    select 1
                    from STAFF_TYPE ste 
                    where ste.staff_type_id = sf.staff_type_id
                      and ste.status = 1
                      and ste.specific_type = 1) '
              when vv_specific_type = 3 then -- tren GSNPP: vv_specific_type
                ' select distinct sf_tmp.istaff_id, sf.staff_id
                from sf_tmp
                join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
                on sf_tmp.istaff_id = musp.user_id
                  and musp.status in (0, 1)
                  and musp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                  and (musp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
                  and musp.inherit_shop_spec_type = 1 -- NPP
                join SHOP sp
                on musp.inherit_shop_id = sp.shop_id
                  and sp.status = 1
                join MAP_USER_SHOP muspp
                on sp.shop_id = muspp.inherit_shop_id
                  and muspp.status in (0, 1)
                  and muspp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                  and (muspp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or muspp.to_date is null)
                join STAFF sf
                on muspp.user_id = sf.staff_id 
                  and sf.status = 1
                  and exists (
                    select 1
                    from STAFF_TYPE ste 
                    where ste.staff_type_id = sf.staff_type_id
                      and ste.status = 1
                      and ste.specific_type = 1) '
              else -- mac dinh NV vv_specific_type = 1
                ' select distinct sf_tmp.istaff_id, sf.staff_id
                from sf_tmp
                join STAFF sf
                on sf_tmp.istaff_id = sf.staff_id '
        end)
        || ')
        --, sp_tmp as (
        --  select distinct musp.user_id as staff_id, musp.inherit_shop_id as shop_id
        --  from MAP_USER_SHOP musp
        --  join isf_tmp 
        --  on musp.user_id = isf_tmp.staff_id
        --  where musp.status in (0, 1)
        --    and musp.from_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
        --    and (musp.to_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)          
        --)
        -- ky dang ky tra thuong hop le
        , kcrd_tmp as (
          select kcrd.ks_id, kcrd.ks_cycle_reward_id
              , trunc(kcrd.from_date) as from_date, trunc(kcrd.to_date) as to_date
              , rank() over (
                  partition by kcrd.ks_id
                  order by trunc(kcrd.to_date) desc) as rk
          from KS_CYCLE_REWARD kcrd
          where kcrd.status = 1 
            and kcrd.ks_id = ' || v_ks_id || '
            and kcrd.to_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'')
            and kcrd.to_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
          order by kcrd.ks_id
        )
        , cr_tmp as (
            select distinct isf_tmp.staff_id, r.customer_id
            from isf_tmp
            join VISIT_PLAN vpn
            on isf_tmp.staff_id = vpn.staff_id
            join (
                select r.routing_id, cr.customer_id
                from ROUTING r
                join ROUTING_CUSTOMER rcr
                on r.routing_id = rcr.routing_id
                  and rcr.status = 1
                  and rcr.start_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                  and (rcr.end_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or rcr.end_date is null)
                  and (nvl(rcr.week1, 0) + nvl(rcr.week2, 0) + nvl(rcr.week3, 0) + nvl(rcr.week4, 0)) > 0
                join CUSTOMER cr
                on rcr.customer_id = cr.customer_id
                  and cr.status = 1
                where r.status = 1
            ) r
            on vpn.routing_id = r.routing_id
            where vpn.status = 1
              and vpn.from_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
              and (vpn.to_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or vpn.to_date is null)
              -- and vpn.shop_id in (select shop_id from sp_tmp)
        )
        , cr_tmp2 as (
          select cr_tmp.staff_id, cr_tmp.customer_id, kcr.ks_cycle_reward_id
              , ks.ks_id, ks.percent_photo_pass
          from cr_tmp
          join KS_CUSTOMER kcr
          on cr_tmp.customer_id = kcr.customer_id
          join KS 
          on kcr.ks_id = ks.ks_id 
            and ks.status in (0, 1) -- l?y c? (ho?t ??ng + t?m ng?ng) trong k?
            and ks.percent_photo_pass is not null
            and ks.ks_id = ' || v_ks_id || '
          where kcr.status = 1
            and kcr.customer_approve_status = 1
            and exists (
                select 1 
                from kcrd_tmp 
                where kcrd_tmp.rk = 1 
                  and kcrd_tmp.ks_id = kcr.ks_id 
                  and kcrd_tmp.ks_cycle_reward_id = kcr.ks_cycle_reward_id)
        )
        , dta_tmp as (
            select cr_tmp2.staff_id, cr_tmp2.ks_id
              , cr_tmp2.customer_id
              , (case when round(count(distinct mdrt.media_item_id) * 100 / nullif(count(distinct mim.media_item_id), 0), 2) 
                            >= cr_tmp2.percent_photo_pass then 1 
                      else 0 end) as is_pass
            from cr_tmp2
            join MEDIA_ITEM mim
            on cr_tmp2.customer_id = mim.object_id
              and mim.ks_cycle_reward_id = cr_tmp2.ks_cycle_reward_id
            left join MEDIA_DISPLAY_RESULT mdrt
            on mim.media_item_id = mdrt.media_item_id
              and mim.display_program_id = mdrt.ks_id
              and mdrt.status = 1
              and mdrt.is_check = 1
              and mdrt.is_mark = 1
              and mdrt.object_type = 4
            where mim.media_type = 0
              and mim.object_type = 4
              and mim.display_program_id = ' || v_ks_id || '
            group by cr_tmp2.staff_id, cr_tmp2.ks_id, cr_tmp2.percent_photo_pass, cr_tmp2.customer_id
        )
        , dta_tmp1 as (
          select isf_tmp.istaff_id, isf_tmp.staff_id
            , count(distinct dta_tmp.customer_id) as tt_customer_pass
          from isf_tmp
          join dta_tmp
          on isf_tmp.staff_id = dta_tmp.staff_id
            and dta_tmp.is_pass = 1
          group by isf_tmp.istaff_id, isf_tmp.staff_id
        )
        select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
            , vkrr.plan_type
            , kqa.plan_value as plan_value
            , nvl(rpt.tt_customer_pass, 0) as gain
        from KPI_REGISTER_HIS vkrr
        join sf_tmp sf 
        on 1 = 1 ' ||
        (case when i_object_type = 2 then 
                ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
              when i_object_type = 4 then 
                ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                  and sf.istaff_id not in (
                      select krhs.object_id
                      from KPI_REGISTER_HIS krhs
                      where krhs.kpi_period = 1
                        and krhs.kpi_period_value = vkrr.kpi_period_value
                        and krhs.object_type = 2 -- NV
                        and krhs.kpi_group_config_id is not null)'
        end) ||
        ' join (
            select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
            from KPI_QUOTA kqat
            where kqat.kpi_period_value = ' || i_kpi_period_value || '
                and kqat.status         = 1
                and kqat.weighted is not null
                and kqat.object_type    = 2
                and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
                and kqat.kpi_config_id = ' || i_kpi_config_id || '
        ) kqa 
        on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
        left join (
          select istaff_id
            , sum(nvl(tt_customer_pass, 0)) as tt_customer_pass
          from dta_tmp1
          group by istaff_id
        ) rpt
        on sf.istaff_id = rpt.istaff_id
        where vkrr.kpi_period = 1 -- chu ky
          and vkrr.kpi_period_value = ' || i_kpi_period_value || '
          and vkrr.object_type = ' || i_object_type || '
          and vkrr.kpi_config_id = ' || i_kpi_config_id || '
          and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
          and vkrr.plan_type = ' || i_plan_type || '
          and kqa.plan_value is not null' ;

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id  = i_kpi_period_value
              and rpt.shop_id   = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id   = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id   = v_dta(indx).staff_id;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          -- t�nh ?i?m cho t?ng ti�u ch� KPI trong b?.
          /*if v_dta(indx).plan_value is null 
              or (v_dta(indx).plan_value <= 0 and v_dta(indx).gain is null )
          then
            v_score := 0;
          elsif v_dta(indx).plan_value <= 0 then
              v_score := round(((1 * nvl(v_dta(indx).weighted, 100))/ 100), 2);
          else 
            if i_max_value is not null and v_dta(indx).gain > i_max_value then
              v_score := round((((i_max_value / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            else 
              v_score := round((((v_dta(indx).gain / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            end if;
          end if;*/
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_CUS_PASS_KS_STAFF_CYCLE;
  
  PROCEDURE P_KPI_DTDB_STAFF_CYCLE (
    i_object_type number,
    i_object_id number,
    i_kpi_period_value number,
    i_kpi_group_config_id number,
    i_kpi_config_id number,
    i_kpi_config_code varchar2,
    i_plan_type number,
    i_kpi_reg_id number,
    i_max_value number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [duy tr� ?i?m b�n; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y c� ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_kpi_period number;
    v_group_column varchar2(100);
    v_rpt_id number(20);
    v_error_type number(2);
    v_error_msg nvarchar2(2000);
    v_params nvarchar2(2000);
    v_score RPT_KPI_CYCLE.SCORE%TYPE;
    v_weighted number;
    v_count_param     number;
    vv_specific_type  STAFF_TYPE.specific_type%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_DTDB_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type is null
        or i_object_id is null
        or v_kpi_period is null
        or i_kpi_period_value is null
        or i_kpi_group_config_id is null
        or i_kpi_config_id is null
        or i_kpi_config_code is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2; 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [1: NVBH; 2: GSNPP; 3: tren GSNPP]
    if vv_specific_type not in (1, 2, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [1, 2, 3]');
      return;
    end if;
    
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    if i_kpi_config_code in ('DTDB_STAFF_PRODUCT') then
        v_group_column := 'product_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 2
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where ptt.product_code in (
                    select kpve.value
                    from KPI_PARAM_VALUE kpve
                    join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                    where kpve.kpi_config_id = ' || i_kpi_config_id || '
                      and kpve.status in (0, 1)
                      and kpm.type = ' || 2 || '
                      and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                      and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('DTDB_STAFF_CAT') then 
        v_group_column := 'cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 1
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 1 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('DTDB_STAFF_SUBCAT') then 
        v_group_column := 'sub_cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 8
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.sub_cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 8 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('DTDB_STAFF_BRAND') then 
        v_group_column := 'brand_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 3
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.brand_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 3 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('DTDB_STAFF_FLAVOUR') then 
        v_group_column := 'flavour_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 4
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.flavour_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 4 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('DTDB_STAFF_PACKING') then 
        v_group_column := 'packing_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 5
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.packing_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 5 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('DTDB_STAFF_UOM') then 
        v_group_column := 'uom1';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 7
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where ptt.uom1 in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 7 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('DTDB_STAFF_VOLUMN') then 
        v_group_column := 'volumn';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 6
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where to_char(ptt.volumn) in (
                  select replace(kpve.value, ''0.'', ''.'') 
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 6 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('DTDB_STAFF_ALL') then
        -- CHAY FULL
        v_group_column := '-1';
    else       
        -- CHAY FULL
        v_group_column := '-1';
    end if;  
    
    v_sql := 
      '-- ds NV tinh KPI
      with sf_tmp as (
        select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
          , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
        from STAFF sf 
        join STAFF_TYPE_TMP ste 
        on ste.staff_id = sf.staff_id
        where sf.status = 1 ' ||
          (case when i_object_type = 2 then 
            ' and sf.staff_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
            ' and ste.staff_type_id = ' || i_object_id || ' '
          end)
      || ' )
      -- ds NV truc thuoc
      , isf_tmp as ( ' || 
      (case when vv_specific_type = 2 then -- GSNPP: vv_specific_type
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join MAP_USER_STAFF musf
              on musf.user_id = sf_tmp.istaff_id
                and musf.status in (0, 1)
                and musf.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (musf.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musf.to_date is null)
              join STAFF sf
              on musf.inherit_staff_id = sf.staff_id
                and sf.status = 1
                and exists (
                  select 1
                  from STAFF_TYPE ste 
                  where ste.staff_type_id = sf.staff_type_id
                    and ste.status = 1
                    and ste.specific_type = 1) '
            when vv_specific_type = 3 then -- tren GSNPP: vv_specific_type
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
              on sf_tmp.istaff_id = musp.user_id
                and musp.status in (0, 1)
                and musp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (musp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
                and musp.inherit_shop_spec_type = 1 -- NPP
              join SHOP sp
              on musp.inherit_shop_id = sp.shop_id
                and sp.status = 1
              join MAP_USER_SHOP muspp
              on sp.shop_id = muspp.inherit_shop_id
                and muspp.status in (0, 1)
                and muspp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (muspp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or muspp.to_date is null)
              join STAFF sf
              on muspp.user_id = sf.staff_id 
                and sf.status = 1
                and exists (
                  select 1
                  from STAFF_TYPE ste 
                  where ste.staff_type_id = sf.staff_type_id
                    and ste.status = 1
                    and ste.specific_type = 1) '
            else -- mac dinh NV vv_specific_type = 1
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join STAFF sf
              on sf_tmp.istaff_id = sf.staff_id '
      end)
      || ')
      select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , kqa.plan_value as plan_value
          , rpt.gain
      from KPI_REGISTER_HIS vkrr
      join sf_tmp sf 
      on 1 = 1 ' ||
      (case when i_object_type = 2 then 
              ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
              ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                and sf.istaff_id not in (
                  select krhs.object_id
                  from KPI_REGISTER_HIS krhs
                  where krhs.kpi_period = 1
                    and krhs.kpi_period_value = vkrr.kpi_period_value
                    and krhs.object_type = 2 -- NV
                    and krhs.kpi_group_config_id is not null)'
       end) ||
      ' join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.status         = 1
              and kqat.weighted is not null
              and kqat.object_type    = 2
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id  = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
      left join (
        select dta.staff_id
          , count(distinct dta.customer_id) as gain
        from (
            select sff.istaff_id as staff_id, rptt.customer_id
              , sum(nvl(rptt.amount_approved, 0)) as amount
            from isf_tmp sff
            join RPT_SALE_PRIMARY_MONTH rptt
            on sff.staff_id = rptt.staff_id
            where rptt.cycle_id = ' || i_kpi_period_value || '
              and rptt.customer_id is not null '
              || (case when v_group_column = '-1' then null 
                       when v_group_column = 'volumn' then
                          ' and exists (select 1 from product ptt where ptt.product_id = rptt.product_id and ptt.' || v_group_column || ' is not null) '
                       else ' and rptt.' || v_group_column || ' is not null ' end) 
              || (case when trim(v_params) is not null then ' and rptt.' || v_params 
                       else null end) || '
            group by sff.istaff_id, rptt.customer_id
            having sum(nvl(rptt.amount_approved, 0)) > 0
        ) dta
        where dta.amount > 0
        group by dta.staff_id
      ) rpt
      on sf.istaff_id = rpt.staff_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type || '
        and kqa.plan_value is not null ';

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          -- t�nh ?i?m cho t?ng ti�u ch� KPI trong b?.
          /*if v_dta(indx).plan_value is null 
              or (v_dta(indx).plan_value <= 0 and v_dta(indx).gain is null )
          then
            v_score := 0;
          elsif v_dta(indx).plan_value <= 0 then
              v_score := round(((1 * nvl(v_dta(indx).weighted, 100))/ 100), 2);
          else 
            if i_max_value is not null and v_dta(indx).gain > i_max_value then
              v_score := round((((i_max_value / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            else 
              v_score := round((((v_dta(indx).gain / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            end if;
          end if;*/
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_DTDB_STAFF_CYCLE;

  PROCEDURE P_KPI_KSCUSRREG_STAFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [kh�ch h�ng ??ng k� ks; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n c? th?; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql         clob; 
    v_kpi_period  number;
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_score       RPT_KPI_CYCLE.SCORE%TYPE;
    v_ks_id       KS.KS_ID%TYPE;
    v_cyc_bdate   date;
    v_cyc_edate   date;
    vv_specific_type STAFF_TYPE.specific_type%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_KSCUSRREG_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type is null
        or i_object_id is null
        or v_kpi_period is null
        or i_kpi_period_value is null
        or i_kpi_group_config_id is null
        or i_kpi_config_id is null
        or i_kpi_config_code is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    begin
      select ce.begin_date, ce.end_date
      into v_cyc_bdate, v_cyc_edate
      from CYCLE ce
      where cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'cycle id not found');
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle|exception: ' 
        || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 200));
      return;
    end;
    
    if v_cyc_bdate is null or v_cyc_edate is null then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'begin date or end date of cycle is null');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2, 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [1: NVBH; 2: GSNPP; 3: tren GSNPP]
    if vv_specific_type not in (1, 2, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [1, 2, 3]');
      return;
    end if;
    
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    -- l?y th�ng tin keyshop
    begin
      select to_number(kpve.value)
      into v_ks_id
      from KPI_PARAM_VALUE kpve
      join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
      where kpve.kpi_config_id = i_kpi_config_id
        and kpve.status in (0, 1)
        and kpm.type = 10
        and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
        and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'not found ks_id');
      return;
    when too_many_rows then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'too many ks_id');
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get ks_id|exception: ' 
          || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 500));
      return;
    end;
    
    v_sql := 
      '-- ds NV tinh KPI
      with sf_tmp as (
        select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
          , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
        from STAFF sf 
        join STAFF_TYPE_TMP ste 
        on ste.staff_id = sf.staff_id
        where sf.status = 1 ' ||
          (case when i_object_type = 2 then 
            ' and sf.staff_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
            ' and ste.staff_type_id = ' || i_object_id || ' '
          end)
      || ' )
      -- ds NV truc thuoc
      , isf_tmp as ( ' || 
      (case when vv_specific_type = 2 then -- GSNPP: vv_specific_type
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join MAP_USER_STAFF musf
              on musf.user_id = sf_tmp.istaff_id
                and musf.status in (0, 1)
                and musf.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (musf.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musf.to_date is null)
              join STAFF sf
              on musf.inherit_staff_id = sf.staff_id
                and sf.status = 1
                and exists (
                  select 1
                  from STAFF_TYPE ste 
                  where ste.staff_type_id = sf.staff_type_id
                    and ste.status = 1
                    and ste.specific_type = 1) '
            when vv_specific_type = 3 then -- tren GSNPP: vv_specific_type
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
              on sf_tmp.istaff_id = musp.user_id
                and musp.status in (0, 1)
                and musp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (musp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
                and musp.inherit_shop_spec_type = 1 -- NPP
              join SHOP sp
              on musp.inherit_shop_id = sp.shop_id
                and sp.status = 1
              join MAP_USER_SHOP muspp
              on sp.shop_id = muspp.inherit_shop_id
                and muspp.status in (0, 1)
                and muspp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (muspp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or muspp.to_date is null)
              join STAFF sf
              on muspp.user_id = sf.staff_id 
                and sf.status = 1
                and exists (
                  select 1
                  from STAFF_TYPE ste 
                  where ste.staff_type_id = sf.staff_type_id
                    and ste.status = 1
                    and ste.specific_type = 1) '
            else -- mac dinh NV vv_specific_type = 1
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join STAFF sf
              on sf_tmp.istaff_id = sf.staff_id '
      end)
      || ')
      , sp_tmp as (
          select distinct sf_tmp.istaff_id as staff_id, musp.inherit_shop_id as shop_id
          from sf_tmp
          join MAP_USER_SHOP musp
          on sf_tmp.istaff_id = musp.user_id
          where musp.status in (0, 1)
              and musp.from_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
              and (musp.to_date >= to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
      )
      -- ky dang ky tra thuong hop le
      , kcrd_tmp as (
        select kcrd.ks_id, kcrd.ks_cycle_reward_id
            , trunc(kcrd.from_date) as from_date, trunc(kcrd.to_date) as to_date
            , rank() over (
                partition by kcrd.ks_id
                order by trunc(kcrd.to_date) desc) as rk
        from KS_CYCLE_REWARD kcrd
        where kcrd.status = 1 
          and kcrd.ks_id = ' || v_ks_id || '
          and kcrd.to_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'')
          and kcrd.to_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
        order by kcrd.ks_id
      )
      , cr_tmp as (
          select isf_tmp.istaff_id, isf_tmp.staff_id, r.customer_id
          from isf_tmp
          join VISIT_PLAN vpn
          on isf_tmp.staff_id = vpn.staff_id
          join (
              select rg.routing_id, cr.customer_id
              from ROUTING rg
              join ROUTING_CUSTOMER rcr
              on rg.routing_id = rcr.routing_id
                and rcr.status = 1
                and rcr.start_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (rcr.end_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or rcr.end_date is null)
                and (nvl(rcr.week1, 0) + nvl(rcr.week2, 0) + nvl(rcr.week3, 0) + nvl(rcr.week4, 0)) > 0
              join CUSTOMER cr
              on rcr.customer_id = cr.customer_id
                and cr.status = 1
              where rg.status = 1
          ) r
          on vpn.routing_id = r.routing_id
          where vpn.status = 1
            and vpn.from_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
            and (vpn.to_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or vpn.to_date is null)
            -- and vpn.shop_id in (select shop_id from sp_tmp)
            and r.customer_id in (
              select kcr.customer_id
              from KS_CUSTOMER kcr
              where kcr.REGISTER_SOURCE = 1 -- HO dang ky
                and exists (select 1 from kcrd_tmp
                            where kcrd_tmp.rk = 1 and kcrd_tmp.ks_id = kcr.ks_id
                              and kcrd_tmp.ks_cycle_reward_id = kcr.ks_cycle_reward_id)
            ) -- khach hang duoc phan bo suat KPI tu HO
          group by isf_tmp.istaff_id, isf_tmp.staff_id, r.customer_id
      )
      select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , kqa.plan_value as plan_value
          , (case when nullif(nvl(sp_map.tt_customer, 0), 0) = 0 and nvl(sf_map.total_cus_reg, 0)  > 0 then 100
                  when nullif(nvl(sp_map.tt_customer, 0), 0) = 0 and nvl(sf_map.total_cus_reg, 0) <= 0 then 0
                  else round(nvl(sf_map.total_cus_reg, 0) * 100 / nullif(nvl(sp_map.tt_customer, 0), 0), 2)
            end) as gain
      from KPI_REGISTER_HIS vkrr
      join sf_tmp sf 
      on 1 = 1 ' ||
      (case when i_object_type = 2 then 
              ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
              ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                and sf.istaff_id not in (
                  select krhs.object_id
                  from KPI_REGISTER_HIS krhs
                  where krhs.kpi_period = 1
                    and krhs.kpi_period_value = vkrr.kpi_period_value
                    and krhs.object_type = 2 -- NV
                    and krhs.kpi_group_config_id is not null)'
       end) ||
      ' join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.status         = 1
              and kqat.weighted is not null
              and kqat.object_type    = 2
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
      left join (
        select sf_tmp.istaff_id, nvl(ksmp_tmp.tt_customer_nv, 0) + nvl(cr.tt_customer_ho, 0) as tt_customer
        from (
          select distinct isf_tmp.istaff_id as istaff_id
          from isf_tmp 
        ) sf_tmp
        left join (
          select sff.istaff_id, sum(nvl(ksmp.quantity, 0)) as tt_customer_nv
          from isf_tmp sff
          left join KS_STAFF_MAP ksmp
          on sff.staff_id = ksmp.staff_id
            and ksmp.status = 1
            and exists (select 1 from kcrd_tmp
                        where kcrd_tmp.rk = 1 and kcrd_tmp.ks_id = ksmp.ks_id
                          and kcrd_tmp.ks_cycle_reward_id = ksmp.ks_cycle_reward_id)
          group by sff.istaff_id
        ) ksmp_tmp on sf_tmp.istaff_id = ksmp_tmp.istaff_id
        left join (
          select istaff_id, count(1) as tt_customer_ho
          from cr_tmp
          group by istaff_id
        ) cr
        on cr.istaff_id = sf_tmp.istaff_id      
      ) sp_map
      on sf.istaff_id = sp_map.istaff_id
      left join (
          select isf_tmp.istaff_id as staff_id
            , count(distinct kcr.customer_id) as total_cus_reg
          from isf_tmp
          join KS_CUSTOMER kcr
          on ((kcr.register_source = 2 and isf_tmp.staff_id = kcr.create_user_id)
            or (kcr.register_source = 1
            and (isf_tmp.staff_id, kcr.customer_id) in (
                select staff_id, customer_id
                from cr_tmp
            )))
          join KS 
          on kcr.ks_id = ks.ks_id
            and ks.status in (0, 1) -- l?y c? (ho?t ??ng + t?m ng?ng) trong k?
            and ks.ks_id = ' || v_ks_id || '
          where kcr.status = 1
            and kcr.customer_approve_status = 1
            and kcr.register_source in (1, 2) -- ca HO + NV
            and kcr.shop_id in (
              select shop_id
              from sp_tmp)
            and exists (
              select 1 
              from kcrd_tmp 
              where kcrd_tmp.rk = 1 and kcrd_tmp.ks_id = kcr.ks_id
                and kcr.ks_cycle_reward_id = kcrd_tmp.ks_cycle_reward_id)
          group by isf_tmp.istaff_id
      ) sf_map
      on sf.istaff_id = sf_map.staff_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type || '
        and kqa.plan_value is not null ';

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          -- t�nh ?i?m cho t?ng ti�u ch� KPI trong b?.
          /*if v_dta(indx).plan_value is null 
              or (v_dta(indx).plan_value <= 0 and v_dta(indx).gain is null )
          then
            v_score := 0;
          elsif v_dta(indx).plan_value <= 0 then
              v_score := round(((1 * nvl(v_dta(indx).weighted, 100))/ 100), 2);
          else 
            if i_max_value is not null and v_dta(indx).gain > i_max_value then
              v_score := round((((i_max_value / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            else 
              v_score := round((((v_dta(indx).gain / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            end if;
          end if;*/
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_KSCUSRREG_STAFF_CYCLE;
  
  PROCEDURE P_KPI_PROMOREG_SFCYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date  date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [kh�ch h�ng tham gia CTKM; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n c? th?; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql         clob;
    v_kpi_period  number;
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_score       RPT_KPI_CYCLE.SCORE%TYPE;
    v_promo_id    PROMOTION_PROGRAM.promotion_program_id%TYPE;
    v_cyc_bdate   date;
    v_cyc_edate   date;
    vv_specific_type STAFF_TYPE.specific_type%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_PROMOREG_SFCYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type is null
        or i_object_id is null
        or v_kpi_period is null
        or i_kpi_period_value is null
        or i_kpi_group_config_id is null
        or i_kpi_config_id is null
        or i_kpi_config_code is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    begin
      select ce.begin_date, ce.end_date
      into v_cyc_bdate, v_cyc_edate
      from CYCLE ce
      where cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'cycle id not found');
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle|exception: ' 
        || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 200));
      return;
    end;
    
    if v_cyc_bdate is null or v_cyc_edate is null then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'begin date or end date of cycle is null');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2, 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [1: NVBH; 2: GSNPP; 3: tren GSNPP]
    if vv_specific_type not in (1, 2, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [1, 2, 3]');
      return;
    end if;
    
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    -- l?y th�ng tin keyshop
    begin
      select to_number(kpve.value)
      into v_promo_id
      from KPI_PARAM_VALUE kpve
      join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
      where kpve.kpi_config_id = i_kpi_config_id
        and kpve.status in (0, 1)
        and kpm.type = 12
        and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
        and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'not found promotion_program_id');
      return;
    when too_many_rows then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'too many promotion_program_id');
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get promotion_program_id|exception: ' 
          || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 500));
      return;
    end;
    
    v_sql := 
      '-- ds NV tinh KPI
      with sf_tmp as (
        select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
          , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
        from STAFF sf 
        join STAFF_TYPE_TMP ste 
        on ste.staff_id = sf.staff_id
        where sf.status = 1 ' ||
          (case when i_object_type = 2 then 
            ' and sf.staff_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
            ' and ste.staff_type_id = ' || i_object_id || ' '
          end)
      || ' )
      -- ds NV truc thuoc
      , isf_tmp as ( ' || 
      (case when vv_specific_type = 2 then -- GSNPP: vv_specific_type
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join MAP_USER_STAFF musf
              on musf.user_id = sf_tmp.istaff_id
                and musf.status in (0, 1)
                and musf.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (musf.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musf.to_date is null)
              join STAFF sf
              on musf.inherit_staff_id = sf.staff_id
                and sf.status = 1
                and exists (
                  select 1
                  from STAFF_TYPE ste 
                  where ste.staff_type_id = sf.staff_type_id
                    and ste.status = 1
                    and ste.specific_type = 1) '
            when vv_specific_type = 3 then -- tren GSNPP: vv_specific_type
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
              on sf_tmp.istaff_id = musp.user_id
                and musp.status in (0, 1)
                and musp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (musp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
                and musp.inherit_shop_spec_type = 1 -- NPP
              join SHOP sp
              on musp.inherit_shop_id = sp.shop_id
                and sp.status = 1
              join MAP_USER_SHOP muspp
              on sp.shop_id = muspp.inherit_shop_id
                and muspp.status in (0, 1)
                and muspp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                and (muspp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or muspp.to_date is null)
              join STAFF sf
              on muspp.user_id = sf.staff_id 
                and sf.status = 1
                and exists (
                  select 1
                  from STAFF_TYPE ste 
                  where ste.staff_type_id = sf.staff_type_id
                    and ste.status = 1
                    and ste.specific_type = 1) '
            else -- mac dinh NV vv_specific_type = 1
              ' select distinct sf_tmp.istaff_id, sf.staff_id
              from sf_tmp
              join STAFF sf
              on sf_tmp.istaff_id = sf.staff_id '
      end)
      || ')
      , sp_tmp as (
          select distinct sf_tmp.istaff_id as staff_id, musp.inherit_shop_id as shop_id
          from sf_tmp
          join MAP_USER_SHOP musp
          on sf_tmp.istaff_id = musp.user_id
          where musp.status in (0, 1)
              and musp.from_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
              and (musp.to_date >= to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
      )
      , dta_tmp as (
        select sf.istaff_id as staff_id
          , sum(nvl(psfmp.quantity_max, 0)) as quantity_max
          , sum(nvl(psfmp.quantity_received, 0)) as quantity_received
        from PROMOTION_PROGRAM ppm
        join PROMOTION_SHOP_MAP pspmp
        on pspmp.promotion_program_id = ppm.promotion_program_id
          and pspmp.status = 1
          and pspmp.from_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
          -- and pspmp.to_date -- [ko check to_date, lay shop_map trong ky da qua]
        join PROMOTION_STAFF_MAP psfmp
        on psfmp.promotion_shop_map_id = pspmp.promotion_shop_map_id
          and psfmp.status = 1
          and psfmp.quantity_max is not null
        join isf_tmp sf
        on sf.staff_id = psfmp.staff_id
          and exists (
            select 1
            from sp_tmp
            where psfmp.shop_id= sp_tmp.shop_id
                and sf.istaff_id= sp_tmp.staff_id
          ) -- xet cho truong hop NVQL => shop thuoc quyen quan ly NVQL
        where ppm.promotion_program_id = ' || v_promo_id || '
        group by sf.istaff_id
      )
      select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , kqa.plan_value as plan_value
          , round(nvl(dta_tmp.quantity_received, 0) * 100 / nullif(nvl(dta_tmp.quantity_max, 0), 0), 2) as gain
      from KPI_REGISTER_HIS vkrr
      join sf_tmp sf 
      on 1 = 1 ' ||
      (case when i_object_type = 2 then 
              ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
              ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                and sf.istaff_id not in (
                  select krhs.object_id
                  from KPI_REGISTER_HIS krhs
                  where krhs.kpi_period = 1
                    and krhs.kpi_period_value = vkrr.kpi_period_value
                    and krhs.object_type = 2 -- NV
                    and krhs.kpi_group_config_id is not null)'
       end) ||
      ' join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.status         = 1
              and kqat.weighted is not null
              and kqat.object_type    = 2
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
      left join dta_tmp
      on sf.istaff_id = dta_tmp.staff_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type || '
        and kqa.plan_value is not null ';

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          -- t�nh ?i?m cho t?ng ti�u ch� KPI trong b?.
          /*if v_dta(indx).plan_value is null 
              or (v_dta(indx).plan_value <= 0 and v_dta(indx).gain is null )
          then
            v_score := 0;
          elsif v_dta(indx).plan_value <= 0 then
              v_score := round(((1 * nvl(v_dta(indx).weighted, 100))/ 100), 2);
          else 
            if i_max_value is not null and v_dta(indx).gain > i_max_value then
              v_score := round((((i_max_value / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            else 
              v_score := round((((v_dta(indx).gain / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            end if;
          end if;*/
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_PROMOREG_SFCYCLE;

  PROCEDURE P_KPI_MM_STAFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [m? m?i; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql           clob; 
    v_kpi_period    number;
    v_group_column  varchar2(100);
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_params      nvarchar2(2000);
    v_score       RPT_KPI_CYCLE.SCORE%TYPE;
    v_count_param number;
    vv_specific_type STAFF_TYPE.specific_type%TYPE;
    v_cycle_seed    CONSTANT number := 3;
    --v_cycle_bf      CYCLE.cycle_id%type;
    v_kpi_relation  varchar2(5); -- loai KPI: AND, OR
    v_num_pro       number := 0;
    
    vv_bccycle_date CYCLE.end_date%type; -- ngay dau chu ky hien tai
    vv_eccycle_date CYCLE.end_date%type; -- ngay cuoi chu ky hien tai
    
    v_pro_name      CONSTANT varchar2(200) := 'P_KPI_MM_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE
    );


    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type   is null
        or i_object_id  is null
        or v_kpi_period is null
        or i_kpi_period_value     is null
        or i_kpi_group_config_id  is null
        or i_kpi_config_id    is null
        or i_kpi_config_code  is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    select trunc(ce.begin_date), trunc(ce.end_date)
    into vv_bccycle_date, vv_eccycle_date
    from CYCLE ce
    where ce.cycle_id= i_kpi_period_value;
    
    -- neu chu ky chua ket thuc, lay ngay hien tai
    if vv_eccycle_date > trunc(sysdate) then
      vv_eccycle_date := trunc(sysdate);
    end if;
    
    -- l?y th�ng tin cycle.
    -- v_cycle_bf := F_GET_CYCLE_SEED_BY_CYCLE(i_kpi_period_value, (-1) * v_cycle_seed); -- l?y tr??c 3 chu k?;
    -- v_cycle_af := F_GET_CYCLE_SEED_BY_CYCLE(i_kpi_period_value, 1); -- l?y sau 1 chu k?;
    
    -- if v_cycle_bf is null then
    --   insert_log_procedure(v_pro_name, NULL, NULL, 3, 'not have cycle before ' || v_cycle_seed || ' time');
    --   return;
    -- end if;
    
    -- if v_cycle_af is null then
    --   insert_log_procedure(v_pro_name, NULL, NULL, 3, 'not have cycle for future');
    --   return;
    -- end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2; 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [1: NVBH; 2: GSNPP; 3: tren GSNPP]
    if vv_specific_type not in (1, 2, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [1, 2, 3]');
      return;
    end if;
    
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    -- ki?m tra m? m?i [V�, HO?C]
    BEGIN
      select trim(upper(kpve.VALUE))
      into v_kpi_relation
      from KPI_PARAM_VALUE kpve
      join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
      where kpve.kpi_config_id = i_kpi_config_id
        and kpve.status in (0, 1)
        and kpm.type = 9
        and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
        and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      v_kpi_relation := 'OR';
    -- WHEN TO_MANY_VALUES THEN
    WHEN OTHERS THEN
      insert_log_procedure(v_pro_name, NULL, NULL, 3
        , 'Error when get KPI relation: . Exception: ' ||v_error_msg);
              
      return;
    END;
    
    if v_kpi_relation not in ('AND', 'OR') then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_relation not in ["AND", "OR"]');
      return;
    end if;
    
    if i_kpi_config_code in ('MM_STAFF_PRODUCT') then
        v_group_column := 'product_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 2
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              where ptt.product_code in (
                    select kpve.value
                    from KPI_PARAM_VALUE kpve
                    join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                    where kpve.kpi_config_id = ' || i_kpi_config_id || '
                      and kpve.status in (0, 1)
                      and kpm.type = ' || 2 || '
                      and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                      and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
            
          if v_kpi_relation = 'AND' then
            select count(distinct ptt.product_id)
            into v_num_pro
            from PRODUCT ptt 
            where ptt.status = 1
              and ptt.product_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = i_kpi_config_id
                    and kpve.status in (0, 1)
                    and kpm.type = 2
                    and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
                    and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null))
            ;
          end if;
        end if;
    elsif i_kpi_config_code in ('MM_STAFF_CAT') then 
        v_group_column := 'cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 1
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 1 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
            
          if v_kpi_relation = 'AND' then
            select count(distinct ptt.cat_id)
            into v_num_pro
            from PRODUCT ptt 
            join PRODUCT_INFO pioo on ptt.cat_id = pioo.product_info_id 
            where ptt.status = 1
              and pioo.product_info_code in (
                select kpve.value
                from KPI_PARAM_VALUE kpve
                join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                where kpve.kpi_config_id = i_kpi_config_id
                  and kpve.status in (0, 1)
                  and kpm.type = 1
                  and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
                  and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null))
            ;
          end if;
        end if;
    elsif i_kpi_config_code in ('MM_STAFF_SUBCAT') then 
        v_group_column := 'sub_cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 8
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.sub_cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 8 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null))
            ) ';
            
          if v_kpi_relation = 'AND' then
            select count(distinct ptt.sub_cat_id)
            into v_num_pro
            from PRODUCT ptt 
            join PRODUCT_INFO pioo on ptt.sub_cat_id = pioo.product_info_id 
            where ptt.status = 1
              and pioo.product_info_code in (
                select kpve.value
                from KPI_PARAM_VALUE kpve
                join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                where kpve.kpi_config_id = i_kpi_config_id
                  and kpve.status in (0, 1)
                  and kpm.type = 8
                  and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
                  and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null))
            ;
          end if;
        end if;
    elsif i_kpi_config_code in ('MM_STAFF_BRAND') then 
        v_group_column := 'brand_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 3
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.brand_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 3 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
            
          if v_kpi_relation = 'AND' then
            select count(distinct ptt.brand_id)
            into v_num_pro
            from PRODUCT ptt 
            join PRODUCT_INFO pioo on ptt.brand_id = pioo.product_info_id 
            where ptt.status = 1
              and pioo.product_info_code in (
                select kpve.value
                from KPI_PARAM_VALUE kpve
                join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                where kpve.kpi_config_id = i_kpi_config_id
                  and kpve.status in (0, 1)
                  and kpm.type = 3
                  and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
                  and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null)
              )
            ;
          end if;
        end if;
    elsif i_kpi_config_code in ('MM_STAFF_FLAVOUR') then 
        v_group_column := 'flavour_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 4
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.flavour_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 4 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
            
          if v_kpi_relation = 'AND' then
            select count(distinct ptt.flavour_id)
            into v_num_pro
            from PRODUCT ptt 
            join PRODUCT_INFO pioo on ptt.flavour_id = pioo.product_info_id 
            where ptt.status = 1
              and pioo.product_info_code in (
                select kpve.value
                from KPI_PARAM_VALUE kpve
                join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                where kpve.kpi_config_id = i_kpi_config_id
                  and kpve.status in (0, 1)
                  and kpm.type = 4
                  and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
                  and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null)
              )
            ;
          end if;
        end if;
    elsif i_kpi_config_code in ('MM_STAFF_PACKING') then 
        v_group_column := 'packing_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 5
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.packing_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 5 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
            
          if v_kpi_relation = 'AND' then
            select count(distinct ptt.packing_id)
            into v_num_pro
            from PRODUCT ptt 
            join PRODUCT_INFO pioo on ptt.packing_id = pioo.product_info_id 
            where ptt.status = 1
              and pioo.product_info_code in (
                select kpve.value
                from KPI_PARAM_VALUE kpve
                join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                where kpve.kpi_config_id = i_kpi_config_id
                  and kpve.status in (0, 1)
                  and kpm.type = 5
                  and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
                  and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null)
              )
            ;
            
          end if;
        end if;
    elsif i_kpi_config_code in ('MM_STAFF_UOM') then 
        v_group_column := 'uom1';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 7
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              where ptt.uom1 in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 7 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
            
          if v_kpi_relation = 'AND' then
            select count(distinct ptt.uom1)
            into v_num_pro
            from PRODUCT ptt 
            where ptt.status = 1
              and ptt.uom1 in (
                select kpve.value
                from KPI_PARAM_VALUE kpve
                join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                where kpve.kpi_config_id = i_kpi_config_id
                  and kpve.status in (0, 1)
                  and kpm.type = 7
                  and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
                  and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null)
              )
            ;
          end if;
        end if;
    elsif i_kpi_config_code in ('MM_STAFF_VOLUMN') then 
        v_group_column := 'volumn';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 6
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              where to_char(ptt.volumn) in (
                  select replace(kpve.value, ''0.'', ''.'')
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 6 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) '; -- THUATTQ hardcode fix bug invalidate number
            
          if v_kpi_relation = 'AND' then
            select count(distinct ptt.volumn)
            into v_num_pro
            from PRODUCT ptt 
            where ptt.status = 1
              and to_char(ptt.volumn) in (
                select replace(kpve.value, '0.', '.')
                from KPI_PARAM_VALUE kpve
                join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                where kpve.kpi_config_id = i_kpi_config_id
                  and kpve.status in (0, 1)
                  and kpm.type = 6
                  and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value
                  and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null)
              )
            ;
          end if;
        end if;    
    elsif i_kpi_config_code in ('MM_STAFF_ALL') then
        -- CHAY FULL
        v_group_column := '-1';
        
        if v_kpi_relation = 'AND' then
          select count(distinct ptt.product_id)
          into v_num_pro
          from product ptt 
          where ptt.status = 1;
        end if;
    else       
        -- CHAY FULL
        v_group_column := '-1';
        
        if v_kpi_relation = 'AND' then
          select count(distinct ptt.product_id)
          into v_num_pro
          from product ptt 
          where ptt.status = 1;
        end if;
    end if;  
    
    if v_kpi_relation = 'AND' and v_num_pro = 0 then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'KPI relation = "AND" nh?ng s? l??ng s?n ph?m = 0, kh�ng ch?y KPI ... ');
      return;
    end if;
    
    if v_kpi_relation = 'OR' then
      if vv_specific_type = 1 then
        v_sql :=  '-- ds NV tinh KPI
          with sf_tmp as (
            select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
              , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
            from STAFF sf 
            join STAFF_TYPE_TMP ste 
            on ste.staff_id = sf.staff_id
            where sf.status = 1 ' ||
              (case when i_object_type = 2 then 
                      ' and sf.staff_id = ' || i_object_id || ' '
                    when i_object_type = 4 then 
                      ' and ste.staff_type_id = ' || i_object_id || ' '
               end) || ' 
          )
          , cus_tmp as (
            select cr.customer_id
            from CUSTOMER cr
            where exists (
                select 1
                from VISIT_PLAN vpn
                join ROUTING rg
                on rg.status in (1, -1) and vpn.routing_id = rg.routing_id
                join ROUTING_CUSTOMER rcr
                on rcr.status = 1  and rcr.routing_id = rg.routing_id
                where 1 = 1
                  and vpn.staff_id in (select istaff_id from sf_tmp)
                  and rcr.customer_id = cr.customer_id
                  and vpn.status = 1
                  and exists (
                    select 1 
                    from CYCLE ce
                    where ce.cycle_id = ' || i_kpi_period_value || '
                      and vpn.from_date  < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                      and (vpn.to_date  >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') or vpn.to_date is null)
                      and rcr.start_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                      and (rcr.end_date >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') or rcr.end_date is null)
                  )
              )
          )
          , cr_sale_tmp as (
            select dta.customer_id, dta.item_sale
            from (
              select cr.customer_id
                , pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' as item_sale                
              from cus_tmp cr
              join PRODUCT pt
              on pt.status in (0, 1) ' || 
                (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) ||
                (case when trim(v_params) is not null then ' and pt.' || v_params else null end) || '
              group by cr.customer_id, pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || '
            ) dta 
            where 1 = 1
              -- co ban hang trong thang hien tai
              and exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_sale
                  and pshs.rpt_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'')
                  and pshs.rpt_date  < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                  and pshs.order_date>= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'')
                  and pshs.order_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
              )
              -- ko ban hang trong 90 ngay ke tu ngay dau thang
              and not exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_sale
                  and pshs.rpt_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.rpt_date  < to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'')
                  and pshs.order_date>= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.order_date < to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'')
              )
          )
          , cr_nsale_tmp as (
            select dta.customer_id, dta.item_nsale
            from (
              select cr.customer_id
                , pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' as item_nsale
              from cus_tmp cr
              join PRODUCT pt
              on pt.status in (0, 1) ' || 
                (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) ||
                (case when trim(v_params) is not null then ' and pt.' || v_params else null end) || '
              group by cr.customer_id, pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || '
            ) dta
            where 1 = 1
              -- co ban hang
              and exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_nsale
                  and pshs.rpt_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
                  and pshs.rpt_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
                  and pshs.order_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
                  and pshs.order_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
              )
              -- ko ban hang trong 90 ngay tu ngay cuoi thang
              and not exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_nsale
                  and pshs.rpt_date >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.rpt_date  < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                  and pshs.order_date>= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.order_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
              )
          )
          , sale_tmp as (
            select cr_sale_tmp.customer_id
              , cr_sale_tmp.item_sale
              , null as item_nsale
            from cr_sale_tmp
            union all
            select cr_nsale_tmp.customer_id
              , null as item_sale
              , cr_nsale_tmp.item_nsale
            from cr_nsale_tmp
          )
          -- ds NV truc thuoc
          , dta_tmp as (
              select dtaa.staff_id
              , sum (case when dtaa.item_sale is not null then 1 
                          when dtaa.item_nsale is not null then -1 
                          else 0 end) as gain
              from (
                select sf_tmp.istaff_id as staff_id, rpt.customer_id
                  , sale_tmp.item_sale, sale_tmp.item_nsale
                from sf_tmp
                join cus_tmp rpt
                on exists (
                    select 1
                    from VISIT_PLAN vpn
                    join ROUTING rg
                    on rg.status in (1, -1) and vpn.routing_id = rg.routing_id
                    join ROUTING_CUSTOMER rcr
                    on rcr.status = 1  and rcr.routing_id = rg.routing_id
                    where vpn.staff_id = sf_tmp.istaff_id
                      and rcr.customer_id = rpt.customer_id
                      and vpn.status = 1
                      and exists (
                        select 1 
                        from CYCLE ce
                        where ce.cycle_id = ' || i_kpi_period_value || '
                          and vpn.from_date  < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                          and (vpn.to_date  >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') or vpn.to_date is null)
                          and rcr.start_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                          and (rcr.end_date >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') or rcr.end_date is null)
                      )
                  )
                left join sale_tmp
                on sale_tmp.customer_id = rpt.customer_id
              ) dtaa
              group by dtaa.staff_id
          )
          select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
              , vkrr.plan_type
              , kqa.plan_value as plan_value
             -- , nvl(dta_tmp.gain, 0) as gain
              , (case when nvl(dta_tmp.gain, 0) < 0 then 0 else nvl(dta_tmp.gain, 0) end) as gain
          from KPI_REGISTER_HIS vkrr
          join sf_tmp sf 
          on 1 = 1 ' ||
            (case when i_object_type = 2 then 
                    ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
                  when i_object_type = 4 then 
                    ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                      and sf.istaff_id not in (
                        select krhs.object_id
                        from KPI_REGISTER_HIS krhs
                        where krhs.kpi_period = 1
                          and krhs.kpi_period_value = vkrr.kpi_period_value
                          and krhs.object_type = 2 -- NV
                          and krhs.kpi_group_config_id is not null)'
             end) ||' 
          join (
              select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
              from KPI_QUOTA kqat
              where kqat.kpi_period_value   = ' || i_kpi_period_value || '
                  and kqat.object_type      = 2
                  and kqat.status           = 1
                  and kqat.weighted is not null
                  and kqat.kpi_register_id  = ' || nvl(i_kpi_reg_id, -1) ||'
                  and kqat.kpi_config_id    = ' || i_kpi_config_id || '
          ) kqa on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
          left join dta_tmp 
          on sf.istaff_id = dta_tmp.staff_id
          where vkrr.kpi_period = 1 -- chu ky
            and vkrr.kpi_period_value = ' || i_kpi_period_value || '
            and vkrr.object_type          = ' || i_object_type || '
            and vkrr.kpi_config_id        = ' || i_kpi_config_id || '
            and vkrr.kpi_group_config_id  = ' || i_kpi_group_config_id || '
            and vkrr.plan_type            = ' || i_plan_type || '
            and kqa.plan_value is not null ';
      elsif vv_specific_type in (2, 3) then
        v_sql := '-- ds NV tinh KPI
          with sf_tmp as (
            select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
              , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
            from STAFF sf 
            join STAFF_TYPE_TMP ste 
            on ste.staff_id = sf.staff_id
            where sf.status = 1 ' ||
              (case when i_object_type = 2 then 
                      ' and sf.staff_id = ' || i_object_id || ' '
                    when i_object_type = 4 then 
                      ' and ste.staff_type_id = ' || i_object_id || ' '
               end) || ' 
          )
          -- ds NPP quan ly
          , isp_tmp as (
            select distinct sf_tmp.istaff_id, sp.shop_id
            from sf_tmp
            join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
            on sf_tmp.istaff_id = musp.user_id
              and musp.status in (0, 1)
              and musp.from_date < to_date(''' || to_char(vv_eccycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
              and (musp.to_date >= to_date(''' || to_char(vv_eccycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') 
                or musp.to_date is null)
              and musp.inherit_shop_spec_type = 1 -- NPP
            join SHOP sp
            on musp.inherit_shop_id = sp.shop_id
              and sp.status = 1
          )
          , cus_tmp as (
            select cr.customer_id
            from CUSTOMER cr
            where cr.status = 1 
              and exists (
                select 1
                from CUSTOMER_SHOP_MAP csmp
                where csmp.customer_id = cr.customer_id
                  and csmp.shop_id in (select shop_id from isp_tmp)
                  and csmp.status = 1 
                  and csmp.from_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1 
                  and (csmp.to_date >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') or csmp.to_date is null)
              )
          )
          , cr_sale_tmp as (
            select dta.customer_id, dta.item_sale
            from (
              select cr.customer_id
                , pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' item_sale
              from cus_tmp cr
              join PRODUCT pt
              on pt.status in (0, 1) ' || 
                (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) ||
                (case when trim(v_params) is not null then ' and pt.' || v_params else null end) || '
              group by cr.customer_id, pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || '
            ) dta 
            where 1 = 1
              -- co ban hang trong thang hien tai
              and exists (
                select 1
                from PRODUCT_SALE_HIS pshs                
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_sale
                  and pshs.rpt_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'')
                  and pshs.rpt_date  < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                  and pshs.order_date>= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'')
                  and pshs.order_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
              )
              -- ko ban hang trong 90 ngay ke tu ngay dau thang
              and not exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_sale
                  and pshs.rpt_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.rpt_date  < to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') 
                  and pshs.order_date>= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.order_date < to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') 
              )
          )
          , cr_nsale_tmp as (
            select dta.customer_id, dta.item_nsale
            from (
              select cr.customer_id
                , pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' as item_nsale
              from cus_tmp cr
              join PRODUCT pt
              on pt.status in (0, 1) ' || 
                (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) ||
                (case when trim(v_params) is not null then ' and pt.' || v_params else null end) || '
              group by cr.customer_id, pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || '
            ) dta
            where 1 = 1
              -- co ban hang
              and exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_nsale
                  and pshs.rpt_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
                  and pshs.rpt_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
                  and pshs.order_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
                  and pshs.order_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
              )
              -- ko ban hang trong 90 ngay tu ngay cuoi thang
              and not exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_nsale
                  and pshs.rpt_date >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.rpt_date  < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                  and pshs.order_date>= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.order_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
              )
          )
          , sale_tmp as (
            select cr_sale_tmp.customer_id
              , cr_sale_tmp.item_sale
              , null as item_nsale
            from cr_sale_tmp
            union all
            select cr_nsale_tmp.customer_id 
              , null as item_sale
              , cr_nsale_tmp.item_nsale
            from cr_nsale_tmp
          )
          , dta_tmp as (
              select dtaa.staff_id
                , sum( case when dtaa.item_sale is not null then 1 
                            when dtaa.item_nsale is not null then -1 
                            else 0 end) as gain
              from (
                select isp_tmp.istaff_id as staff_id, rpt.customer_id
                  , sale_tmp.item_sale, sale_tmp.item_nsale
                from isp_tmp
                join cus_tmp rpt
                on exists (
                    select 1
                    from CUSTOMER_SHOP_MAP csmp
                    where rpt.customer_id = csmp.customer_id
                      and isp_tmp.shop_id = csmp.shop_id
                      and csmp.status = 1 
                      and csmp.from_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1 
                      and (csmp.to_date >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') or csmp.to_date is null)
                  )
                left join sale_tmp
                on sale_tmp.customer_id = rpt.customer_id
              ) dtaa
              group by dtaa.staff_id
          )
          select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
              , vkrr.plan_type
              , kqa.plan_value as plan_value
              --, nvl(dta_tmp.gain, 0) as gain
              , (case when nvl(dta_tmp.gain, 0) < 0 then 0 else nvl(dta_tmp.gain, 0) end) as gain
          from KPI_REGISTER_HIS vkrr
          join sf_tmp sf 
          on 1 = 1 ' ||
            (case when i_object_type = 2 then 
                    ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
                  when i_object_type = 4 then 
                    ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                      and sf.istaff_id not in (
                        select krhs.object_id
                        from KPI_REGISTER_HIS krhs
                        where krhs.kpi_period = 1
                          and krhs.kpi_period_value = vkrr.kpi_period_value
                          and krhs.object_type = 2 -- NV
                          and krhs.kpi_group_config_id is not null)'
             end) || ' 
          join (
              select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
              from KPI_QUOTA kqat
              where kqat.kpi_period_value   = ' || i_kpi_period_value || '
                  and kqat.object_type      = 2
                  and kqat.status           = 1
                  and kqat.weighted is not null
                  and kqat.kpi_register_id  = ' || nvl(i_kpi_reg_id, -1) ||'
                  and kqat.kpi_config_id    = ' || i_kpi_config_id || '
          ) kqa on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
          left join dta_tmp
          on sf.istaff_id = dta_tmp.staff_id
          where vkrr.kpi_period = 1 -- chu ky
            and vkrr.kpi_period_value = ' || i_kpi_period_value || '
            and vkrr.object_type          = ' || i_object_type || '
            and vkrr.kpi_config_id        = ' || i_kpi_config_id || '
            and vkrr.kpi_group_config_id  = ' || i_kpi_group_config_id || '
            and vkrr.plan_type            = ' || i_plan_type || '
            and kqa.plan_value is not null ';
      end if;
    elsif v_kpi_relation = 'AND' then
      if vv_specific_type = 1 then
        v_sql :=  '-- ds NV tinh KPI
          with sf_tmp as (
            select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
              , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
            from STAFF sf 
            join STAFF_TYPE_TMP ste 
            on ste.staff_id = sf.staff_id
            where sf.status = 1 ' ||
              (case when i_object_type = 2 then 
                      ' and sf.staff_id = ' || i_object_id || ' '
                    when i_object_type = 4 then 
                      ' and ste.staff_type_id = ' || i_object_id || ' '
               end) || ' 
          )
          , cus_tmp as (
            select cr.customer_id
            from CUSTOMER cr
            where exists (
                select 1
                from VISIT_PLAN vpn
                join ROUTING rg
                on rg.status in (1, -1) and vpn.routing_id = rg.routing_id
                join ROUTING_CUSTOMER rcr
                on rcr.status = 1  and rcr.routing_id = rg.routing_id
                where 1 = 1
                  and vpn.staff_id in (select istaff_id from sf_tmp)
                  and rcr.customer_id = cr.customer_id
                  and vpn.status = 1
                  and exists (
                    select 1 
                    from CYCLE ce
                    where ce.cycle_id = ' || i_kpi_period_value || '
                      and vpn.from_date  < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                      and (vpn.to_date  >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') or vpn.to_date is null)
                      and rcr.start_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                      and (rcr.end_date >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') or rcr.end_date is null)
                  )
              )
          )
          , cr_sale_tmp as (
            select dta.customer_id, dta.item_sale
            from (
              select cr.customer_id
                , pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' as item_sale
              from cus_tmp cr
              join PRODUCT pt
              on pt.status in (0, 1) ' || 
                (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) ||
                (case when trim(v_params) is not null then ' and pt.' || v_params else null end) || '
              group by cr.customer_id, pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || '
            ) dta
            where 1 = 1
              -- co ban hang trong thang hien tai
              and exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_sale
                  and pshs.rpt_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'')
                  and pshs.rpt_date  < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                  and pshs.order_date>= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'')
                  and pshs.order_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
              )
              -- ko ban hang trong 90 ngay ke tu ngay dau thang
              and not exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_sale
                  and pshs.rpt_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.rpt_date  < to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') 
                  and pshs.order_date>= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.order_date < to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') 
              )
          )
          , cr_nsale_tmp as (
            select dta.customer_id, dta.item_nsale
            from (
              select cr.customer_id
                , pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' as item_nsale
              from cus_tmp cr
              join PRODUCT pt
              on pt.status in (0, 1) ' || 
                (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) ||
                (case when trim(v_params) is not null then ' and pt.' || v_params else null end) || '
              group by cr.customer_id, pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || '
            ) dta
            where 1 = 1
              -- co ban hang
              and exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_nsale
                  and pshs.rpt_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
                  and pshs.rpt_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
                  and pshs.order_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
                  and pshs.order_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
              )
              -- ko ban hang trong 90 ngay tu ngay cuoi thang
              and not exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_nsale
                  and pshs.rpt_date >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.rpt_date  < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                  and pshs.order_date>= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.order_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
              )
          )
          , sale_tmp as (
            select customer_id, 1 as item_sale
            from cr_sale_tmp
            group by customer_id
            having count(distinct item_sale) >= ' || v_num_pro || '
            union all
            select customer_id, -1 as item_sale
            from cr_nsale_tmp
            group by customer_id
            having count(distinct item_nsale) >= ' || v_num_pro || '
          )
          -- ds NV truc thuoc
          , dta_tmp as (
                select sf_tmp.istaff_id as staff_id
                  , sum(nvl(sale_tmp.item_sale, 0)) as gain
                from sf_tmp
                join cus_tmp rpt
                on exists (
                    select 1
                    from VISIT_PLAN vpn
                    join ROUTING rg
                    on rg.status in (1, -1) and vpn.routing_id = rg.routing_id
                    join ROUTING_CUSTOMER rcr
                    on rcr.status = 1  and rcr.routing_id = rg.routing_id
                    where vpn.staff_id = sf_tmp.istaff_id
                      and rcr.customer_id = rpt.customer_id
                      and vpn.status = 1
                      and exists (
                        select 1 
                        from CYCLE ce
                        where ce.cycle_id = ' || i_kpi_period_value || '
                          and vpn.from_date  < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                          and (vpn.to_date  >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') or vpn.to_date is null)
                          and rcr.start_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                          and (rcr.end_date >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') or rcr.end_date is null)
                      )
                  )
                left join sale_tmp 
                on sale_tmp.customer_id = rpt.customer_id
                group by sf_tmp.istaff_id
          )
          select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
              , vkrr.plan_type
              , kqa.plan_value as plan_value
              --, nvl(dta_tmp.gain, 0) as gain
              , (case when nvl(dta_tmp.gain, 0) < 0 then 0 else nvl(dta_tmp.gain, 0) end) as gain
          from KPI_REGISTER_HIS vkrr
          join sf_tmp sf 
          on 1 = 1 ' ||
            (case when i_object_type = 2 then 
                    ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
                  when i_object_type = 4 then 
                    ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                      and sf.istaff_id not in (
                        select krhs.object_id
                        from KPI_REGISTER_HIS krhs
                        where krhs.kpi_period = 1
                          and krhs.kpi_period_value = vkrr.kpi_period_value
                          and krhs.object_type = 2 -- NV
                          and krhs.kpi_group_config_id is not null)'
             end) ||' 
          join (
              select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
              from KPI_QUOTA kqat
              where kqat.kpi_period_value   = ' || i_kpi_period_value || '
                  and kqat.object_type      = 2
                  and kqat.status           = 1
                  and kqat.weighted is not null
                  and kqat.kpi_register_id  = ' || nvl(i_kpi_reg_id, -1) ||'
                  and kqat.kpi_config_id    = ' || i_kpi_config_id || '
          ) kqa on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
          left join dta_tmp 
          on sf.istaff_id = dta_tmp.staff_id
          where vkrr.kpi_period = 1 -- chu ky
            and vkrr.kpi_period_value = ' || i_kpi_period_value || '
            and vkrr.object_type          = ' || i_object_type || '
            and vkrr.kpi_config_id        = ' || i_kpi_config_id || '
            and vkrr.kpi_group_config_id  = ' || i_kpi_group_config_id || '
            and vkrr.plan_type            = ' || i_plan_type || '
            and kqa.plan_value is not null ';
      elsif vv_specific_type in (2, 3) then
        v_sql := '-- ds NV tinh KPI
          with sf_tmp as (
            select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
              , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
            from STAFF sf 
            join STAFF_TYPE_TMP ste 
            on ste.staff_id = sf.staff_id
            where sf.status = 1 ' ||
              (case when i_object_type = 2 then 
                      ' and sf.staff_id = ' || i_object_id || ' '
                    when i_object_type = 4 then 
                      ' and ste.staff_type_id = ' || i_object_id || ' '
               end) || ' 
          )
          -- ds NPP quan ly
          , isp_tmp as (
            select distinct sf_tmp.istaff_id, sp.shop_id
            from sf_tmp
            join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
            on sf_tmp.istaff_id = musp.user_id
              and musp.status in (0, 1)
              and musp.from_date < to_date(''' || to_char(vv_eccycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
              and (musp.to_date >= to_date(''' || to_char(vv_eccycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') 
                or musp.to_date is null)
              and musp.inherit_shop_spec_type = 1 -- NPP
            join SHOP sp
            on musp.inherit_shop_id = sp.shop_id
              and sp.status = 1
          )
          , cus_tmp as (
            select cr.customer_id
            from CUSTOMER cr
            where cr.status = 1 
              and exists (
                select 1
                from CUSTOMER_SHOP_MAP csmp
                where csmp.customer_id = cr.customer_id
                  and csmp.shop_id in (select shop_id from isp_tmp)
                  and csmp.status = 1 
                  and csmp.from_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1 
                  and (csmp.to_date >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') or csmp.to_date is null)
              )
          )
          , cr_sale_tmp as (
            select dta.customer_id, dta.item_sale
            from (
              select cr.customer_id
                , pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' as item_sale
              from cus_tmp cr
              join PRODUCT pt
              on pt.status in (0, 1) ' || 
                (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) ||
                (case when trim(v_params) is not null then ' and pt.' || v_params else null end) || '
              group by cr.customer_id, pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || '
            ) dta
            where 1 = 1
              -- co ban hang trong thang hien tai
              and exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_sale
                  and pshs.rpt_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'')
                  and pshs.rpt_date  < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                  and pshs.order_date>= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'')
                  and pshs.order_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
              )
              -- ko ban hang trong 90 ngay ke tu ngay dau thang
              and not exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_sale
                  and pshs.rpt_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.rpt_date  < to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') 
                  and pshs.order_date>= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.order_date < to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') 
              )
          )
          , cr_nsale_tmp as (
            select dta.customer_id, dta.item_nsale
            from (
              select cr.customer_id
                , pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' as item_nsale
              from cus_tmp cr
              join PRODUCT pt
              on pt.status in (0, 1) ' || 
                (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) ||
                (case when trim(v_params) is not null then ' and pt.' || v_params else null end) || '
              group by cr.customer_id, pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || '
            ) dta
            where 1 = 1
              -- co ban hang
              and exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_nsale
                  and pshs.rpt_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
                  and pshs.rpt_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
                  and pshs.order_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
                  and pshs.order_date >= to_date('''||to_char(vv_bccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90 
              )
              -- ko ban hang trong 90 ngay tu ngay cuoi thang
              and not exists (
                select 1
                from PRODUCT_SALE_HIS pshs
                join PRODUCT pt on pshs.product_id = pt.product_id
                  and pt.status in (0, 1)
                where pshs.customer_id = dta.customer_id
                  and pt.' || (case when v_group_column = '-1' then 'product_id' else v_group_column end) || ' = dta.item_nsale
                  and pshs.rpt_date >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.rpt_date  < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
                  and pshs.order_date>= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') - 90
                  and pshs.order_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1
              )
          )
          , sale_tmp as (
            select customer_id, 1 as item_sale
            from cr_sale_tmp
            group by customer_id
            having count(distinct item_sale) >= ' || v_num_pro || '
            union all
            select customer_id, -1 as item_sale
            from cr_nsale_tmp
            group by customer_id
            having count(distinct item_nsale) >= ' || v_num_pro || '
          )
          , dta_tmp as (
              select isp_tmp.istaff_id as staff_id
                , sum(nvl(sale_tmp.item_sale, 0)) as gain
              from isp_tmp
              join cus_tmp rpt
              on exists (
                  select 1
                  from CUSTOMER_SHOP_MAP csmp
                  where rpt.customer_id = csmp.customer_id
                    and isp_tmp.shop_id = csmp.shop_id
                    and csmp.status = 1 
                    and csmp.from_date < to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') + 1 
                    and (csmp.to_date >= to_date('''||to_char(vv_eccycle_date, 'yyyy-mm-dd')||''', ''yyyy-mm-dd'') or csmp.to_date is null)
                )
              left join sale_tmp
              on sale_tmp.customer_id = rpt.customer_id
              group by isp_tmp.istaff_id
          )
          select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
              , vkrr.plan_type
              , kqa.plan_value as plan_value
              --, nvl(dta_tmp.gain, 0) as gain
              , (case when nvl(dta_tmp.gain, 0) < 0 then 0 else nvl(dta_tmp.gain, 0) end) as gain
          from KPI_REGISTER_HIS vkrr
          join sf_tmp sf 
          on 1 = 1 ' ||
            (case when i_object_type = 2 then 
                    ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
                  when i_object_type = 4 then 
                    ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                      and sf.istaff_id not in (
                        select krhs.object_id
                        from KPI_REGISTER_HIS krhs
                        where krhs.kpi_period = 1
                          and krhs.kpi_period_value = vkrr.kpi_period_value
                          and krhs.object_type = 2 -- NV
                          and krhs.kpi_group_config_id is not null)'
             end) || ' 
          join (
              select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
              from KPI_QUOTA kqat
              where kqat.kpi_period_value   = ' || i_kpi_period_value || '
                  and kqat.object_type      = 2
                  and kqat.status           = 1
                  and kqat.weighted is not null
                  and kqat.kpi_register_id  = ' || nvl(i_kpi_reg_id, -1) ||'
                  and kqat.kpi_config_id    = ' || i_kpi_config_id || '
          ) kqa on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
          left join dta_tmp
          on sf.istaff_id = dta_tmp.staff_id
          where vkrr.kpi_period = 1 -- chu ky
            and vkrr.kpi_period_value = ' || i_kpi_period_value || '
            and vkrr.object_type          = ' || i_object_type || '
            and vkrr.kpi_config_id        = ' || i_kpi_config_id || '
            and vkrr.kpi_group_config_id  = ' || i_kpi_group_config_id || '
            and vkrr.plan_type            = ' || i_plan_type || '
            and kqa.plan_value is not null ';
      end if;
    end if;
    
    dbms_output.enable(100000000);
    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            --RETURNING RPT_KPI_CYCLE
            --BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
   rollback;
   insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_MM_STAFF_CYCLE;

  PROCEDURE P_KPI_PER_CUSKS_STAFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [% ?i?m tr?ng b�y ??t chu?n; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n c? th?; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql         clob; 
    v_kpi_period  number;
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_score       RPT_KPI_CYCLE.SCORE%TYPE;
    v_ks_id       KS.KS_ID%TYPE;
    v_cyc_bdate   date;
    v_cyc_edate   date;
    vv_specific_type STAFF_TYPE.specific_type%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_PER_CUSKS_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if    i_object_type           is null
      or  i_object_id             is null
      or  v_kpi_period            is null
      or  i_kpi_period_value      is null
      or  i_kpi_group_config_id   is null
      or  i_kpi_config_id         is null
      or  i_kpi_config_code       is null
      or  i_plan_type             is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    begin
        select ce.begin_date, ce.end_date
        into v_cyc_bdate, v_cyc_edate
        from cycle ce
        where cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'cycle id not found');
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle|exception: ' 
        || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 200));
      return;
    end;
    
    if v_cyc_bdate is null or v_cyc_edate is null then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'begin date or end date of cycle is null');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [1: NVBH; 2: GSNPP; 3: tren GSNPP]
    if vv_specific_type not in (1, 2, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [1, 2, 3]');
      return;
    end if;
    
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    -- l?y th�ng tin keyshop
    begin
      select to_number(kpve.value)
      into v_ks_id
      from KPI_PARAM_VALUE kpve
      join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
      where kpve.kpi_config_id = i_kpi_config_id
        and kpve.status in (0, 1)
        and kpm.type = 10
        and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
        and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'not found ks_id');
      return;
    when too_many_rows then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'too many ks_id');
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get ks_id|exception: ' 
          || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 500));
      return;
    end;
    
    v_sql := 
        '-- ds NV tinh KPI
        with sf_tmp as (
          select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
            , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
          from STAFF sf 
          join STAFF_TYPE_TMP ste 
          on ste.staff_id = sf.staff_id
          where sf.status = 1 
          ' ||
            (case when i_object_type = 2 then 
              ' and sf.staff_id = ' || i_object_id || ' '
              when i_object_type = 4 then 
              ' and ste.staff_type_id = ' || i_object_id || ' '
            end)
        || ' )
        -- ds NV truc thuoc
        , isf_tmp as ( ' || 
        (case when vv_specific_type = 2 then -- GSNPP: vv_specific_type
                ' select distinct sf_tmp.istaff_id, sf.staff_id
                from sf_tmp
                join MAP_USER_STAFF musf
                on musf.user_id = sf_tmp.istaff_id
                  and musf.status in (0, 1)
                  and musf.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                  and (musf.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musf.to_date is null)
                join STAFF sf
                on musf.inherit_staff_id = sf.staff_id
                  and sf.status = 1
                  and exists (
                    select 1
                    from STAFF_TYPE ste 
                    where ste.staff_type_id = sf.staff_type_id
                      and ste.status = 1
                      and ste.specific_type = 1) '
              when vv_specific_type = 3 then -- tren GSNPP: vv_specific_type
                ' select distinct sf_tmp.istaff_id, sf.staff_id
                from sf_tmp
                join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
                on sf_tmp.istaff_id = musp.user_id
                  and musp.status in (0, 1)
                  and musp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                  and (musp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
                  and musp.inherit_shop_spec_type = 1 -- NPP
                join SHOP sp
                on musp.inherit_shop_id = sp.shop_id
                  and sp.status = 1
                join MAP_USER_SHOP muspp
                on sp.shop_id = muspp.inherit_shop_id
                  and muspp.status in (0, 1)
                  and muspp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                  and (muspp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or muspp.to_date is null)
                join STAFF sf
                on muspp.user_id = sf.staff_id 
                  and sf.status = 1
                  and exists (
                    select 1
                    from STAFF_TYPE ste 
                    where ste.staff_type_id = sf.staff_type_id
                      and ste.status = 1
                      and ste.specific_type = 1) '
              else -- mac dinh NV vv_specific_type = 1
                ' select distinct sf_tmp.istaff_id, sf.staff_id
                from sf_tmp
                join STAFF sf
                on sf_tmp.istaff_id = sf.staff_id '
        end)
        || ')
        --, sp_tmp as (
        --  select distinct musp.user_id as staff_id, musp.inherit_shop_id as shop_id
        --  from MAP_USER_SHOP musp
        --  join isf_tmp 
        --  on musp.user_id = isf_tmp.staff_id
        --  where musp.status in (0, 1)
        --    and musp.from_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
        --    and (musp.to_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)          
        --)
        -- ky dang ky tra thuong hop le
        , kcrd_tmp as (
          select kcrd.ks_id, kcrd.ks_cycle_reward_id
              , trunc(kcrd.from_date) as from_date, trunc(kcrd.to_date) as to_date
              , rank() over (
                  partition by kcrd.ks_id
                  order by trunc(kcrd.to_date) desc) as rk
          from KS_CYCLE_REWARD kcrd
          where kcrd.status = 1 
            and kcrd.ks_id = ' || v_ks_id || '
            and kcrd.to_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'')
            and kcrd.to_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
          order by kcrd.ks_id
        )
        , cr_tmp as (
            select distinct isf_tmp.staff_id, r.customer_id
            from isf_tmp
            join VISIT_PLAN vpn
            on isf_tmp.staff_id = vpn.staff_id
            join (
                select r.routing_id, cr.customer_id
                from ROUTING r
                join ROUTING_CUSTOMER rcr
                on r.routing_id = rcr.routing_id
                  and rcr.status = 1
                  and rcr.start_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                  and (rcr.end_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or rcr.end_date is null)
                  and (nvl(rcr.week1, 0) + nvl(rcr.week2, 0) + nvl(rcr.week3, 0) + nvl(rcr.week4, 0)) > 0
                join CUSTOMER cr
                on rcr.customer_id = cr.customer_id
                  and cr.status = 1
                where r.status = 1
            ) r
            on vpn.routing_id = r.routing_id
            where vpn.status = 1
              and vpn.from_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
              and (vpn.to_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or vpn.to_date is null)
              -- and vpn.shop_id in (select shop_id from sp_tmp)
        )
        , cr_tmp2 as (
          select cr_tmp.staff_id, cr_tmp.customer_id, kcr.ks_cycle_reward_id
              , ks.ks_id, ks.percent_photo_pass
          from cr_tmp
          join KS_CUSTOMER kcr
          on cr_tmp.customer_id = kcr.customer_id
          join KS 
          on kcr.ks_id = ks.ks_id 
            and ks.status in (0, 1) -- l?y c? (ho?t ??ng + t?m ng?ng) trong k?
            and ks.percent_photo_pass is not null
            and ks.ks_id = ' || v_ks_id || '
          where kcr.status = 1
            and kcr.customer_approve_status = 1
            and exists (
                select 1 
                from kcrd_tmp 
                where kcrd_tmp.rk = 1 
                  and kcrd_tmp.ks_id = kcr.ks_id 
                  and kcrd_tmp.ks_cycle_reward_id = kcr.ks_cycle_reward_id)
        )
        , dta_tmp as (
            select cr_tmp2.staff_id, cr_tmp2.ks_id
              , cr_tmp2.customer_id
              , (case when round(count(distinct mdrt.media_item_id) * 100 / 
                                    nullif(count(distinct mim.media_item_id), 0), 2) 
                              >= cr_tmp2.percent_photo_pass 
                  then 1
                  else 0 end
                ) as is_pass
            from cr_tmp2
            left join MEDIA_ITEM mim
            on cr_tmp2.customer_id = mim.object_id
              and mim.ks_cycle_reward_id = cr_tmp2.ks_cycle_reward_id
              and mim.media_type = 0
              and mim.object_type = 4
              and mim.display_program_id = ' || v_ks_id || '
            left join MEDIA_DISPLAY_RESULT mdrt
            on mim.media_item_id = mdrt.media_item_id
              and mim.display_program_id = mdrt.ks_id
              and mdrt.status = 1
              and mdrt.is_check = 1
              and mdrt.is_mark = 1
              and mdrt.object_type = 4
            -- where 1 = 1
            group by cr_tmp2.staff_id, cr_tmp2.ks_id, cr_tmp2.customer_id, cr_tmp2.percent_photo_pass
        )
        select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
            , vkrr.plan_type
            , kqa.plan_value as plan_value
            , round(nvl(rpt.gain, 0), 2) as gain
        from KPI_REGISTER_HIS vkrr
        join sf_tmp sf 
        on 1 = 1 ' ||
        (case when i_object_type = 2 then 
                ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
              when i_object_type = 4 then 
                ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                  and sf.istaff_id not in (
                      select krhs.object_id
                      from KPI_REGISTER_HIS krhs
                      where krhs.kpi_period = 1
                        and krhs.kpi_period_value = vkrr.kpi_period_value
                        and krhs.object_type = 2 -- NV
                        and krhs.kpi_group_config_id is not null)'
         end) ||
        ' join (
            select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
            from KPI_QUOTA kqat
            where kqat.kpi_period_value = ' || i_kpi_period_value || '
                and kqat.status         = 1
                and kqat.weighted is not null
                and kqat.object_type    = 2
                and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
                and kqat.kpi_config_id = ' || i_kpi_config_id || '
        ) kqa 
        on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
        /* left join (
          select isf_tmp.istaff_id
            , count(distinct case when dta_tmp.is_pass = 1 then dta_tmp.customer_id else null end) * 100 / 
                nullif(count(distinct dta_tmp.customer_id), 0) as gain
          from isf_tmp
          join dta_tmp
          on isf_tmp.staff_id = dta_tmp.staff_id
          group by isf_tmp.istaff_id
        ) rpt */
        -- fix lay them sum NV len GS [ref P_KPI_CUS_PASS_KS_STAFF_CYCLE]
        left join (
          select isf_tmp.istaff_id
            , sum(nvl(dta.pass_cus, 0)) * 100 / 
                nullif(sum(nvl(tt_cus, 0)), 0) as gain
          from isf_tmp
          join (
            select staff_id
              , count(distinct case when is_pass = 1 then customer_id else null end) as pass_cus
              , count(distinct customer_id) as tt_cus
            from dta_tmp
            group by staff_id
          ) dta
          on isf_tmp.staff_id = dta.staff_id
          group by isf_tmp.istaff_id
        ) rpt
        on sf.istaff_id = rpt.istaff_id
        where vkrr.kpi_period = 1 -- chu ky
          and vkrr.kpi_period_value = ' || i_kpi_period_value || '
          and vkrr.object_type = ' || i_object_type || '
          and vkrr.kpi_config_id = ' || i_kpi_config_id || '
          and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
          and vkrr.plan_type = ' || i_plan_type || '
          and kqa.plan_value is not null' ;

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          -- t�nh ?i?m cho t?ng ti�u ch� KPI trong b?.
          /*if v_dta(indx).plan_value is null 
              or (v_dta(indx).plan_value <= 0 and v_dta(indx).gain is null )
          then
            v_score := 0;
          elsif v_dta(indx).plan_value <= 0 then
              v_score := round(((1 * nvl(v_dta(indx).weighted, 100))/ 100), 2);
          else 
            if i_max_value is not null and v_dta(indx).gain > i_max_value then
              v_score := round((((i_max_value / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            else 
              v_score := round((((v_dta(indx).gain / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            end if;
          end if;*/
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_PER_CUSKS_STAFF_CYCLE;
  
  PROCEDURE P_KPI_RET_ORDER_STAFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [h�ng tr? v?; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_kpi_period    number;
    v_group_column  varchar2(100);
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_params  nvarchar2(2000);
    v_score   RPT_KPI_CYCLE.SCORE%TYPE;
    v_weighted number;
    vv_cycle_bdate cycle.begin_date%type;
    vv_cycle_edate cycle.begin_date%type;
    v_count_param number;
    vv_specific_type STAFF_TYPE.specific_type%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_RET_ORDER_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE,
      gain_ir         RPT_KPI_CYCLE.DONE_IR%TYPE,
      gain_or         RPT_KPI_CYCLE.DONE_OR%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if   i_object_type is null
      or i_object_id is null
      or v_kpi_period is null
      or i_kpi_period_value is null
      or i_kpi_group_config_id is null
      or i_kpi_config_id is null
      or i_kpi_config_code is null
      or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    -- 
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2; 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [1: NVBH; 2: GSNPP; 3: tren GSNPP]
    if vv_specific_type not in (1, 2, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [1, 2, 3]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    begin
      select trunc(begin_date), trunc(end_date)
      into vv_cycle_bdate, vv_cycle_edate
      from CYCLE ce
      where ce.cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'can''t find cycle with cycle_id = ' || i_kpi_period_value);
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle info with cycle_id =  ' || i_kpi_period_value 
          || '. Exception: ' || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000));
      return;
    end;
    
    if i_kpi_config_code in ('RET_ORDER_STAFF_PRODUCT') then
        v_group_column := 'product_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 2
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where ptt.product_code in (
                    select kpve.value
                    from KPI_PARAM_VALUE kpve
                    join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                    where kpve.kpi_config_id = ' || i_kpi_config_id || '
                      and kpve.status in (0, 1)
                      and kpm.type = ' || 2 || '
                      and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                      and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;  
    elsif i_kpi_config_code in ('RET_ORDER_STAFF_CAT') then 
        v_group_column := 'cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 1
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 1 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('RET_ORDER_STAFF_SUBCAT') then 
        v_group_column := 'sub_cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 8
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value  or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.sub_cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 8 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('RET_ORDER_STAFF_BRAND') then 
        v_group_column := 'brand_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 3
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.brand_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 3 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('RET_ORDER_STAFF_FLAVOUR') then 
        v_group_column := 'flavour_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 4
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.flavour_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 4 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('RET_ORDER_STAFF_PACKING') then 
        v_group_column := 'packing_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 5
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.packing_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 5 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('RET_ORDER_STAFF_UOM') then 
        v_group_column := 'uom1';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 7
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where ptt.uom1 in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 7 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;  
    elsif i_kpi_config_code in ('RET_ORDER_STAFF_VOLUMN') then 
        v_group_column := 'volumn';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 6
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where to_char(ptt.volumn) in (
                  select replace(kpve.value, ''0.'', ''.'')
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 6 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('RET_ORDER_STAFF_ALL') then
        -- CHAY FULL
        v_group_column := '-1';
    else       
        -- CHAY FULL
        v_group_column := '-1';
    end if;
    
    if vv_specific_type = 1 then
      v_sql := 
        'select sf.staff_id, sf.shop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
            , vkrr.plan_type
            , kqa.plan_value as plan_value
            , round(rpt.total_quantity_re * 1000/ nullif(rpt.total_quantity, 0), 2) as gain
            , 0 as gain_ir
            , 0 as gain_or
        from KPI_REGISTER_HIS vkrr
        join STAFF sf 
        on sf.status = 1 ' ||
        (case when i_object_type = 2 then 
                ' and sf.staff_id = vkrr.object_id and sf.staff_id = ' || i_object_id || ' '
              when i_object_type = 4 then 
                ' and exists (
                    select 1
                    from STAFF_TYPE ste 
                    where ste.staff_type_id = sf.staff_type_id
                      and ste.status = 1 
                      and ste.staff_type_id = vkrr.object_id
                      and ste.staff_type_id = ' || i_object_id || ')
                  and sf.staff_id not in (
                      select krhs.object_id
                      from KPI_REGISTER_HIS krhs
                      where krhs.kpi_period = 1
                        and krhs.kpi_period_value = vkrr.kpi_period_value
                        and krhs.object_type = 2 -- NV
                        and krhs.kpi_group_config_id is not null)'
         end) || ' 
        join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.status       = 1
              and kqat.weighted is not null
              and kqat.object_type  = 2
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id = ' || i_kpi_config_id || '
        ) kqa on kqa.object_id = sf.staff_id and kqa.kpi_config_id = vkrr.kpi_config_id
        left join (
          select sor.staff_id
            , sum(case when sor.type in (0, 1) then nvl(sodl.quantity, 0) else - 1 * nvl(sodl.quantity, 0) end) as total_quantity
          from SALE_ORDER sor
          join SALE_ORDER_DETAIL sodl
          on sor.sale_order_id = sodl.sale_order_id
          join PRODUCT pt
          on sodl.product_id = pt.product_id
          where sor.cycle_id  = ' || i_kpi_period_value || '
            and sor.approved  in (1)
            and sor.type      in (0, 1, 2) -- 0: ??n h�ng b�n ?� th?c hi?n tr? l?i; 1: ??n h�ng b�n nh?ng ch?a tr?; 2: ??n tr? h�ng
            and sor.amount    > 0 -- lay don co doanh so
            and sor.order_date >= to_date(''' || to_char(vv_cycle_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') ' || 
            (case when v_group_column = '-1' then null 
                  else ' and pt.' || v_group_column || ' is not null ' end) ||
            (case when trim(v_params) is not null then ' and pt.' || v_params 
                  else null end) ||'
          group by sor.staff_id
        ) rpt
        on sf.staff_id = rpt.staff_id
        left join (
              select prl.staff_id, ce.cycle_id
                , sum(nvl(prdl.quantity, 0)) as total_quantity_re
              from PRODUCT_RECALL prl
              join PRODUCT_RECALL_DETAIL prdl
              on prl.product_recall_id = prdl.product_recall_id
              join PRODUCT pt
              on prdl.product_id = pt.product_id
                and pt.status in (0, 1)
              join CYCLE ce
              on ce.status = 1
                and ce.cycle_id = ' || i_kpi_period_value || '
                and prl.recall_date >= trunc(ce.begin_date)
                and prl.recall_date <  trunc(ce.end_date) + 1
                and prdl.recall_date >= trunc(ce.begin_date)
                and prdl.recall_date <  trunc(ce.end_date) + 1
              where prl.type = 1
                and prl.status in (0, 1, 2, 3, 4) -- <> trang thai huy
                --and prdl.is_free_item = 0
                ' ||(case when v_group_column = '-1' then null 
                          else ' and pt.' || v_group_column || ' is not null ' end) 
                  ||(case when trim(v_params) is not null then ' and pt.' || v_params 
                          else null end) ||'
              group by prl.staff_id, ce.cycle_id
        ) prll 
        on    prll.staff_id = sf.staff_id
        where vkrr.kpi_period = 1 -- chu ky
          and vkrr.kpi_period_value = ' || i_kpi_period_value || '
          and vkrr.object_type = ' || i_object_type || '
          and vkrr.kpi_config_id = ' || i_kpi_config_id || '
          and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
          and vkrr.plan_type = ' || i_plan_type || '
          and kqa.plan_value is not null ';
    elsif vv_specific_type in (2, 3) then
      v_sql := 
        '-- ds NV tinh KPI
        with sf_tmp as (
          select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
            , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
          from STAFF sf 
          join STAFF_TYPE_TMP ste 
          on ste.staff_id = sf.staff_id
          where sf.status = 1 ' ||
            (case when i_object_type = 2 then 
              ' and sf.staff_id = ' || i_object_id || ' '
              when i_object_type = 4 then 
              ' and ste.staff_type_id = ' || i_object_id || ' '
            end)
        || ' )
        -- ds NV truc thuoc
        , isf_tmp as ( ' || 
        (case when vv_specific_type = 2 then -- GSNPP: vv_specific_type
                ' select distinct sf_tmp.istaff_id, sf.staff_id
                from sf_tmp
                join MAP_USER_STAFF musf
                on musf.user_id = sf_tmp.istaff_id
                  and musf.status in (0, 1)
                  and musf.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                  and (musf.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musf.to_date is null)
                join STAFF sf
                on musf.inherit_staff_id = sf.staff_id
                  and sf.status = 1
                  and exists (
                    select 1
                    from STAFF_TYPE ste 
                    where ste.staff_type_id = sf.staff_type_id
                      and ste.status = 1
                      and ste.specific_type = 1) '
              when vv_specific_type = 3 then -- tren GSNPP: vv_specific_type
                ' select distinct sf_tmp.istaff_id, sf.staff_id
                from sf_tmp
                join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
                on sf_tmp.istaff_id = musp.user_id
                  and musp.status in (0, 1)
                  and musp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                  and (musp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
                  and musp.inherit_shop_spec_type = 1 -- NPP
                join SHOP sp
                on musp.inherit_shop_id = sp.shop_id
                  and sp.status = 1
                join MAP_USER_SHOP muspp
                on sp.shop_id = muspp.inherit_shop_id
                  and muspp.status in (0, 1)
                  and muspp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                  and (muspp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or muspp.to_date is null)
                join STAFF sf
                on muspp.user_id = sf.staff_id 
                  and sf.status = 1
                  and exists (
                    select 1
                    from STAFF_TYPE ste 
                    where ste.staff_type_id = sf.staff_type_id
                      and ste.status = 1
                      and ste.specific_type = 1) '
        end)
        || ')
        , isp_tmp as (
          select distinct sf_tmp.istaff_id, sp.shop_id
          from sf_tmp
          join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
          on sf_tmp.istaff_id = musp.user_id
            and musp.status in (0, 1)
            and musp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
            and (musp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
            and musp.inherit_shop_spec_type = 1 -- NPP
          join SHOP sp
          on musp.inherit_shop_id = sp.shop_id
            and sp.status = 1
        )
        select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
              , vkrr.plan_type
              , kqa.plan_value as plan_value
              , round(rpt_ret.total_quantity_re * 1000/ nullif(rpt.total_quantity, 0), 2) as gain
              , 0 as gain_ir
              , 0 as gain_or
          from KPI_REGISTER_HIS vkrr
          join sf_tmp sf 
          on 1 = 1 ' ||
          (case when i_object_type = 2 then 
                  ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
                when i_object_type = 4 then 
                  ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                    and sf.istaff_id not in (
                      select krhs.object_id
                      from KPI_REGISTER_HIS krhs
                      where krhs.kpi_period = 1
                        and krhs.kpi_period_value = vkrr.kpi_period_value
                        and krhs.object_type = 2 -- NV
                        and krhs.kpi_group_config_id is not null)'
          end) ||
          ' join (
            select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
            from KPI_QUOTA kqat
            where kqat.kpi_period_value = ' || i_kpi_period_value || '
                and kqat.status       = 1
                and kqat.weighted is not null
                and kqat.object_type  = 2
                and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
                and kqat.kpi_config_id = ' || i_kpi_config_id || '
          ) kqa 
          on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
          left join (
            select sff.istaff_id as staff_id
              , sum((case when sor.type in (0, 1) then nvl(sodl.quantity, 0) else 0 end)) as total_quantity
            from isf_tmp sff
            join SALE_ORDER sor
            on sor.staff_id = sff.staff_id
            join SALE_ORDER_DETAIL sodl
            on sor.sale_order_id = sodl.sale_order_id
            join PRODUCT pt
            on sodl.product_id = pt.product_id
              and pt.status in (0, 1)
            where sor.cycle_id  = ' || i_kpi_period_value || '
              and sor.approved  in (1)
              and sor.type      in (0, 1, 2) -- 0: ??n h�ng b�n ?� th?c hi?n tr? l?i; 1: ??n h�ng b�n nh?ng ch?a tr?; 2: ??n tr? h�ng
              and sor.amount    > 0 -- lay don co doanh so
              and sor.order_date >= to_date(''' || to_char(vv_cycle_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') ' || 
              (case when v_group_column = '-1' then null 
                    else ' and pt.' || v_group_column || ' is not null ' end) ||
              (case when trim(v_params) is not null then ' and pt.' || v_params 
                    else null end) ||'
            group by sff.istaff_id
          ) rpt
          on sf.istaff_id = rpt.staff_id
          left join (
            select isp_tmp.istaff_id as staff_id
              , sum(nvl(pvdl.quantity, 0)) as total_quantity_re
            from isp_tmp
            join PO_VNM pvm
            on isp_tmp.shop_id = pvm.shop_id
            join PO_VNM_DETAIL pvdl
            on pvm.po_vnm_id = pvdl.po_vnm_id
              and pvdl.product_type = 4 -- hang doi tra, hong
            join PRODUCT pt
            on pvdl.product_id = pt.product_id
              and pt.status in (0, 1)
            where pvm.type = 1 -- don PO DVKH
              and pvm.object_type = 1 -- NPP
              and pvm.status in (1, 2) -- dang nhap; da nhap, da tra xong
              and pvm.po_vnm_date  < to_date(''' || to_char(vv_cycle_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
              and pvm.po_vnm_date >= to_date(''' || to_char(vv_cycle_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') ' || 
              (case when v_group_column = '-1' then null 
                    else ' and pt.' || v_group_column || ' is not null ' end) ||
              (case when trim(v_params) is not null then ' and pt.' || v_params 
                    else null end) ||'
            group by isp_tmp.istaff_id
          ) rpt_ret
          on sf.istaff_id = rpt_ret.staff_id
          where vkrr.kpi_period = 1 -- chu ky
            and vkrr.kpi_period_value = ' || i_kpi_period_value || '
            and vkrr.object_type = ' || i_object_type || '
            and vkrr.kpi_config_id = ' || i_kpi_config_id || '
            and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
            and vkrr.plan_type = ' || i_plan_type || '
            and kqa.plan_value is not null ';
    end if;    

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          -- t�nh ?i?m cho t?ng ti�u ch� KPI trong b?.
          /*if v_dta(indx).plan_value is null 
              or (v_dta(indx).plan_value <= 0 and v_dta(indx).gain is null )
          then
            v_score := 0;
          elsif v_dta(indx).plan_value <= 0 then
              v_score := round(((1 * nvl(v_dta(indx).weighted, 100))/ 100), 2);
          else 
            if i_max_value is not null and v_dta(indx).gain > i_max_value then
              v_score := round((((i_max_value / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            else 
              v_score := round((((v_dta(indx).gain / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            end if;
          end if;*/
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_RET_ORDER_STAFF_CYCLE;

  PROCEDURE P_KPI_TT_STAFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [t?ng tr??ng; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_kpi_period    number;
    v_group_column  varchar2(100);
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg nvarchar2(2000);
    v_params    nvarchar2(2000);
    v_score     RPT_KPI_CYCLE.SCORE%TYPE;
    v_weighted  number;
    v_cycle_begin_date cycle.begin_date%type;
    v_count_param number;
    vv_specific_type STAFF_TYPE.specific_type%TYPE;
    v_cycle_bf cycle.cycle_id%type;
    v_cur_year cycle.year%type;
    v_cur_num cycle.num%type;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_TT_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE,
      gain_ir         RPT_KPI_CYCLE.DONE_IR%TYPE,
      gain_or         RPT_KPI_CYCLE.DONE_OR%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type is null
        or i_object_id is null
        or v_kpi_period is null
        or i_kpi_period_value is null
        or i_kpi_group_config_id is null
        or i_kpi_config_id is null
        or i_kpi_config_code is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    -- l?y k? c�ng k? n?m tr??c
    begin
      select trunc(ce.year), ce.num
      into v_cur_year, v_cur_num
      from cycle ce
      where ce.cycle_id = i_kpi_period_value;
      
      if v_cur_year is null or v_cur_num is null then
        insert_log_procedure(v_pro_name, NULL, NULL, 3, 'cannot find year or num of cycle ' || i_kpi_period_value);
        return;
      end if;
      
      select ce.cycle_id
      into v_cycle_bf
      from cycle ce
      where ce.year = add_months(v_cur_year, -12)
        and ce.num = v_cur_num;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'cannot find before cycle: ' || i_kpi_period_value);
      return;
    end;
    
    if v_cycle_bf is null then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'cannot find before cycle: ' || i_kpi_period_value);
      return;
    end if;
    
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2; 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [2: GSNPP; 3: tren GSNPP]
    -- ko cap nhat cho NVBH
    if vv_specific_type not in (2, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [2, 3]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    begin
      select trunc(begin_date)
      into v_cycle_begin_date
      from cycle ce
      where ce.cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'can''t find cycle with cycle_id = ' || i_kpi_period_value);
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle info with cycle_id =  ' || i_kpi_period_value 
          || '. Exception: ' || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000));
      return;
    end;
    
    if i_kpi_config_code in ('TT_STAFF_PRODUCT') then
        v_group_column := 'product_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 2
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where ptt.product_code in (
                    select kpve.value
                    from KPI_PARAM_VALUE kpve
                    join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                    where kpve.kpi_config_id = ' || i_kpi_config_id || '
                      and kpve.status in (0, 1)
                      and kpm.type = ' || 2 || '
                      and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                      and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;  
    elsif i_kpi_config_code in ('TT_STAFF_CAT') then 
        v_group_column := 'cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 1
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 1 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('TT_STAFF_SUBCAT') then 
        v_group_column := 'sub_cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 8
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.sub_cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 8 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('TT_STAFF_BRAND') then 
        v_group_column := 'brand_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 3
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.brand_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 3 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('TT_STAFF_FLAVOUR') then 
        v_group_column := 'flavour_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 4
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.flavour_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 4 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('TT_STAFF_PACKING') then 
        v_group_column := 'packing_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 5
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              join product_info pioo on ptt.packing_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 5 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('TT_STAFF_UOM') then 
        v_group_column := 'uom1';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 7
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where ptt.uom1 in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 7 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('TT_STAFF_VOLUMN') then 
        v_group_column := 'volumn';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 6
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from product ptt 
              where to_char(ptt.volumn) in (
                  select replace(kpve.value, ''0.'', ''.'')
                  from KPI_PARAM_VALUE kpve
                  join kpi_param kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 6 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('TT_STAFF_ALL') then
        -- CHAY FULL
        v_group_column := '-1';
    else       
        -- CHAY FULL
        v_group_column := '-1';
    end if;  
    
    v_sql := 
      '-- ds NV tinh KPI
      with sf_tmp as (
        select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
          , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
        from STAFF sf 
        join STAFF_TYPE_TMP ste 
        on ste.staff_id = sf.staff_id
        where sf.status = 1 
          ' ||(case when i_object_type = 2 then 
                      ' and sf.staff_id = ' || i_object_id || ' '
                    when i_object_type = 4 then 
                      ' and ste.staff_type_id = ' || i_object_id || ' '
              end)
      || ' )
      -- ds NV truc thuoc
      , isp_tmp as (
          select distinct sf_tmp.istaff_id, sp.shop_id
          from sf_tmp
          join MAP_USER_SHOP musp -- lay danh sach shop NV quan ly
          on sf_tmp.istaff_id = musp.user_id
            and musp.status in (0, 1)
            and musp.user_spec_type in (2, 3) -- NVQL
            and musp.inherit_shop_spec_type = 1 -- NPP
            and musp.from_date < to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
            and (musp.to_date >= to_date(''' || to_char(i_input_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null) 
            -- [DTYC lay tai thoi gian xet]
          join SHOP sp
          on musp.inherit_shop_id = sp.shop_id
            and sp.status in (0, 1)
      )
      select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , kqa.plan_value as plan_value
          ,(case when (rpt.quantity_bef is null or rpt.quantity_bef <= 0) 
                  and rpt.quantity_cur > 0
                then 100
                when rpt.quantity_bef > 0 then
                  nvl(round(rpt.quantity_cur * 100/ nullif(rpt.quantity_bef, 0), 2), 0)
                else 0 end) as gain
          , 0 as gain_ir
          , 0 as gain_or
      from KPI_REGISTER_HIS vkrr
      join sf_tmp sf 
      on 1 = 1 ' ||
        (case when i_object_type = 2 then 
                ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
              when i_object_type = 4 then 
                ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                  and sf.istaff_id not in (
                    select krhs.object_id
                    from KPI_REGISTER_HIS krhs
                    where krhs.kpi_period = 1
                      and krhs.kpi_period_value = vkrr.kpi_period_value
                      and krhs.object_type = 2 -- NV
                      and krhs.kpi_group_config_id is not null)'
         end) ||
      ' join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.status         = 1
              and kqat.weighted is not null
              and kqat.object_type    = 2
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
      left join (
          select sff.istaff_id as staff_id--, ce.cycle_id
            , sum(case when ce.cycle_id = ' || i_kpi_period_value || ' then nvl(eodl.quantity, 0) else 0 end) quantity_cur
            , sum(case when ce.cycle_id = ' || v_cycle_bf         || ' then nvl(eodl.quantity, 0) else 0 end) quantity_bef
            , 0 as gain_ir, 0 as gain_or
          from isp_tmp sff
          join EXP_ORDER eor
          on sff.shop_id = eor.shop_id
          join EXP_ORDER_DETAIL eodl
          on eor.exp_order_id = eodl.exp_order_id
            and eodl.is_free_item = 0
          join CYCLE ce
          on ce.status = 1
            and eor.tran_date >= trunc(ce.begin_date)
            and eor.tran_date < trunc(ce.end_date) + 1
            and ce.cycle_id in (' || i_kpi_period_value || ', ' || v_cycle_bf || ')
          join PRODUCT ptt
          on ptt.product_id = eodl.product_id
            and ptt.status in (0, 1)
          where eor.status = 0
            '|| (case when v_group_column = '-1' then null 
                      else ' and ptt.' || v_group_column || ' is not null ' end) ||'
            '|| (case when trim(v_params) is not null then ' and eodl.' || v_params 
                      else null end) ||' 
          group by sff.istaff_id--, ce.cycle_id
      ) rpt
      on sf.istaff_id = rpt.staff_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type || '
        and kqa.plan_value is not null ';

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1500, 1500));
  end P_KPI_TT_STAFF_CYCLE;
  
  PROCEDURE P_KPI_BUY_STAFF_CYCLE (
    i_object_type         number,
    i_object_id           number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id       number,
    i_kpi_config_code     varchar2,
    i_plan_type           number,
    i_kpi_reg_id          number,
    i_max_value           number,
    i_input_date          date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [doanh s?, s?n l??ng nh?p; npp; chu k?];
    @author: thuattq1
    
    @params:  
      i_object_type         : Lo?i ??i t??ng: [2; 4]: [GS c? th?; lo?i NV GS].
      i_object_id           : ID NPP.
      i_kpi_period_value    : ID gi� tr? k?.
      i_kpi_group_config_id : ID nh�m KPI.
      i_kpi_config_id       : ID KPI.
      i_kpi_config_code     : M� KPI.
      i_plan_type           : lo?i ph�n b?: 1: ko ph�n b? (l?y t? SALE_PLAN); 2: c� ph�n b? (KPI_QUOTA).
      i_kpi_reg_id          : ID KPI_REGISTER.
      i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql           clob; 
    v_kpi_period    number;
    v_group_column  varchar2(100);
    v_rpt_id        number(20);
    v_error_type    number(2);
    v_error_msg     nvarchar2(2000);
    v_params        nvarchar2(2000);
    v_score         RPT_KPI_CYCLE.SCORE%TYPE;
    v_weighted      number;
    v_atual_column  varchar2(50);
    v_count_param   number;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_BUY_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE,
      gain_ir         RPT_KPI_CYCLE.DONE_IR%TYPE,
      gain_or         RPT_KPI_CYCLE.DONE_OR%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type is null
        or i_object_id is null
        or v_kpi_period is null
        or i_kpi_period_value is null
        or i_kpi_group_config_id is null
        or i_kpi_config_id is null
        or i_kpi_config_code is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2, 4]');
      return;
    end if;
    
    if i_kpi_config_code in (
         'BUY_AMOUNT_STAFF_PRODUCT' , 'BUY_AMOUNT_STAFF_CAT'    , 'BUY_AMOUNT_STAFF_SUBCAT'
       , 'BUY_AMOUNT_STAFF_BRAND'   , 'BUY_AMOUNT_STAFF_FLAVOUR', 'BUY_AMOUNT_STAFF_PACKING'
       , 'BUY_AMOUNT_STAFF_UOM'     , 'BUY_AMOUNT_STAFF_VOLUMN' , 'BUY_AMOUNT_STAFF_ALL'
    ) then
         
      v_atual_column := 'amount';
    elsif i_kpi_config_code in (
         'BUY_QUANTITY_STAFF_PRODUCT' , 'BUY_QUANTITY_STAFF_CAT'    , 'BUY_QUANTITY_STAFF_SUBCAT'
       , 'BUY_QUANTITY_STAFF_BRAND'   , 'BUY_QUANTITY_STAFF_FLAVOUR', 'BUY_QUANTITY_STAFF_PACKING'
       , 'BUY_QUANTITY_STAFF_UOM'     , 'BUY_QUANTITY_STAFF_VOLUMN' , 'BUY_QUANTITY_STAFF_ALL'
    ) then
         
      v_atual_column := 'quantity';
    else
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'invalidate kpi config code');
      return;
    end if;
    
    if i_kpi_config_code in ('BUY_AMOUNT_STAFF_PRODUCT', 'BUY_QUANTITY_STAFF_PRODUCT') then
        v_group_column := 'product_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 2
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              where ptt.product_code in (
                    select kpve.value
                    from KPI_PARAM_VALUE kpve
                    join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                    where kpve.kpi_config_id = ' || i_kpi_config_id || '
                      and kpve.status in (0, 1)
                      and kpm.type = ' || 2 || '
                      and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                      and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;  
    elsif i_kpi_config_code in ('BUY_AMOUNT_STAFF_CAT', 'BUY_QUANTITY_STAFF_CAT') then 
        v_group_column := 'cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 1
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 1 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('BUY_AMOUNT_STAFF_SUBCAT', 'BUY_QUANTITY_STAFF_SUBCAT') then 
        v_group_column := 'sub_cat_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 8
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.sub_cat_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 8 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('BUY_AMOUNT_STAFF_BRAND', 'BUY_QUANTITY_STAFF_BRAND') then 
        v_group_column := 'brand_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 3
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params  := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.brand_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 3 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('BUY_AMOUNT_STAFF_FLAVOUR', 'BUY_QUANTITY_STAFF_FLAVOUR') then 
        v_group_column := 'flavour_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 4
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.flavour_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 4 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('BUY_AMOUNT_STAFF_PACKING', 'BUY_QUANTITY_STAFF_PACKING') then 
        v_group_column := 'packing_id';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 5
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              join PRODUCT_INFO pioo on ptt.packing_id = pioo.product_info_id 
              where pioo.product_info_code in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 5 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('BUY_AMOUNT_STAFF_UOM', 'BUY_QUANTITY_STAFF_UOM') then 
        v_group_column := 'uom1';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 7
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              where ptt.uom1 in (
                  select kpve.value
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 7 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;  
    elsif i_kpi_config_code in ('BUY_AMOUNT_STAFF_VOLUMN', 'BUY_QUANTITY_STAFF_VOLUMN') then 
        v_group_column := 'volumn';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
        where kpve.kpi_config_id = i_kpi_config_id
          and kpve.status in (0, 1)
          and kpm.type = 6
          and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
          and (kpve.TO_KPI_PERIOD_VALUE >=  i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
          
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select ptt.product_id 
              from PRODUCT ptt 
              where to_char(ptt.volumn) in (
                  select replace(kpve.value, ''0.'', ''.'')
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm on kpm.kpi_param_id = kpve.kpi_param_id and kpm.status = 1
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpm.type = ' || 6 || '
                    and kpve.FROM_KPI_PERIOD_VALUE <= ' || i_kpi_period_value || '
                    and (kpve.TO_KPI_PERIOD_VALUE >= ' || i_kpi_period_value || ' or kpve.TO_KPI_PERIOD_VALUE is null)
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('BUY_AMOUNT_STAFF_ALL', 'BUY_QUANTITY_STAFF_ALL') then
        -- CHAY FULL
        v_group_column := '-1';
    else
        -- CHAY FULL
        v_group_column := '-1';
    end if;  
    
    v_sql := 
      'with sf_tmp as (
          select distinct sf.staff_id, sf.staff_type_id, sf.shop_id
            , sp.shop_id as inherit_shop_id
          from STAFF sf
          join MAP_USER_SHOP musp
          on musp.user_id = sf.staff_id
            and musp.status in (0, 1)
            and musp.from_date < to_date('''|| to_char(i_input_date, 'yyyy-mm-dd') ||''', ''yyyy-mm-dd'') + 1
            and (musp.to_date >= to_date('''|| to_char(i_input_date, 'yyyy-mm-dd') ||''', ''yyyy-mm-dd'') or musp.to_date is null)' ||
            -- GS chi lay NPP quan ly cuoi cung
          ' and musp.inherit_shop_spec_type = 1 /* NPP */
          join SHOP sp 
          on sp.shop_id = musp.inherit_shop_id
            and sp.status = 1
          where sf.status = 1 '||
            (case when i_object_type = 2 then 
                    ' and sf.staff_id = ' || i_object_id || ' '
                  when i_object_type = 4 then 
                    ' and exists (
                        select 1 from STAFF_TYPE ste 
                        where ste.staff_type_id = sf.staff_type_id
                          and ste.status = 1 
                          and ste.staff_type_id = ' || i_object_id || ')'
             end) || '
      )
      , sp_tmp as (
          select spn.shop_id /* lay ke hoach nhap cua NPP */
              , pt.product_id, nullif(pt.convfact, 0) as convfact
              , sum(nvl(spn.'||v_atual_column||', 0)) as plan
          from SALE_PLAN spn
          join PRODUCT pt
            on spn.product_id = pt.product_id
          where spn.cycle_id = ' || i_kpi_period_value || '
              and spn.'||v_atual_column||' is not null 
              and spn.object_type = 3
              and spn.type = 2
              and spn.status = 1
              '|| (case when v_group_column = '-1' then null else ' and pt.' || v_group_column || ' is not null ' end) ||'
              '|| (case when trim(v_params) is not null then ' and pt.' || v_params else null end) ||' 
          group by spn.shop_id
            , pt.product_id, pt.convfact
      )
      , rpt_tmp as (
          select eor.shop_id
            , pt.product_id, nullif(pt.convfact, 0) as convfact
            , sum(nvl(eodl.' || v_atual_column || ', 0)) as gain 
          from EXP_ORDER eor
          join EXP_ORDER_DETAIL eodl
          on eor.exp_order_id = eodl.exp_order_id
            and eodl.is_free_item = 0
          join PRODUCT pt
          on eodl.product_id = pt.product_id
          join CYCLE ce
          on ce.status = 1
            and eor.tran_date >= trunc(ce.begin_date)
            and eor.tran_date < trunc(ce.end_date) + 1
            and ce.cycle_id = ' || i_kpi_period_value || '
          where eor.status = 0 '
            || (case when v_group_column = '-1' then null 
                  else ' and pt.' || v_group_column || ' is not null ' end) 
            || (case when trim(v_params) is not null then ' and pt.' || v_params else null end) ||' 
          group by eor.shop_id
            , pt.product_id, pt.convfact
      )
      , dta_tmp as (
          select sf.staff_id, sf.staff_type_id, sf.shop_id
            , sum(nvl(spn.plan, 0)) as plan
            , sum(nvl(rpt.gain, 0)) as gain
          from sf_tmp sf
          left join (
            select shop_id
              , '|| (case when v_atual_column = 'quantity' then
                              'round(sum(nvl(plan/convfact, 0)), 2)'
                          else 'sum(nvl(plan, 0)) ' 
                     end)|| ' as plan
            from sp_tmp
            group by shop_id
          ) spn 
          on spn.shop_id = sf.inherit_shop_id 
          left join (
            select shop_id
              , '|| (case when v_atual_column = 'quantity' then
                            'round(sum(nvl(gain/convfact, 0)), 2) as gain '
                          else 
                            'sum(nvl(gain, 0)) as gain '
                     end)|| '
            from rpt_tmp
            group by shop_id
          ) rpt
          on sf.inherit_shop_id = rpt.shop_id
          group by sf.staff_id, sf.staff_type_id, sf.shop_id
      )
      select sf.staff_id as staff_id, sf.shop_id as shop_id
          , nvl(kqa.weighted, vkrr.weighted) as weighted
          , vkrr.max_value
          , vkrr.plan_type
          , ' ||(case when i_plan_type = 2 then ' kqa.plan_value ' 
                      when i_plan_type = 1 then ' sf.plan ' 
                      else ' null ' end)||' as plan_value
          , sf.gain as gain
          , null as gain_ir
          , null as gain_or
      from KPI_REGISTER_HIS vkrr
      join dta_tmp sf
      on 1 = 1 ' ||
      (case when i_object_type = 2 then 
              ' and sf.staff_id = vkrr.object_id '
            when i_object_type = 4 then 
              ' and sf.staff_type_id = vkrr.object_id 
                and sf.staff_id not in (
                  select krhs.object_id
                  from KPI_REGISTER_HIS krhs
                  where krhs.kpi_period = 1
                    and krhs.kpi_period_value = vkrr.kpi_period_value
                    and krhs.object_type = 2 -- NV
                    and krhs.kpi_group_config_id is not null)'
       end) || ' 
      join (
          select kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.object_type = 2
              and kqat.status = 1
              and kqat.weighted is not null
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa 
      on kqa.object_id = sf.staff_id and kqa.kpi_config_id = vkrr.kpi_config_id 
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type
        /*|| (case when i_plan_type = 2 then 'and kqa.plan_value is not null ' 
              when i_plan_type = 1 then 'and spn.plan is not null ' 
              else null end)*/
    ;
    
    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- npp
              and rpt.object_id = v_dta(indx).staff_id;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id;
          elsif v_error_type = 1 
            -- and v_dta(indx).plan_value is not null -- voi KPI cho NV Quanly ??i x? null, 0 nh? nhau
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               -- done_ir, done_or, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             -- v_dta(indx).gain_ir,
             -- v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;shop_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).shop_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_BUY_STAFF_CYCLE;
  
  PROCEDURE P_KPI_AVG_KPI_SCORE_SHOP_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date  date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [KPI NPP chu?n; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n c? th?; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql         clob; 
    v_kpi_period  number;
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_score       RPT_KPI_CYCLE.SCORE%TYPE;
    v_kpi_group_id  KPI_GROUP_CONFIG.KPI_GROUP_CONFIG_ID%TYPE;
    v_cyc_bdate   date;
    v_cyc_edate   date;
    vv_specific_type STAFF_TYPE.specific_type%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_AVG_KPI_SCORE_SHOP_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type is null
        or i_object_id is null
        or v_kpi_period is null
        or i_kpi_period_value is null
        or i_kpi_group_config_id is null
        or i_kpi_config_id is null
        or i_kpi_config_code is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    begin
      select ce.begin_date, ce.end_date
      into v_cyc_bdate, v_cyc_edate
      from CYCLE ce
      where cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'cycle id not found');
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle|exception: ' 
        || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 200));
      return;
    end;
    
    if v_cyc_bdate is null or v_cyc_edate is null then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'begin date or end date of cycle is null');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2, 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [2: GSNPP; 3: tren GSNPP]
    if vv_specific_type not in (2, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [2, 3]');
      return;
    end if;
    
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    -- l?y th�ng tin param
    begin
      select to_number(kpve.value)
      into v_kpi_group_id
      from KPI_PARAM_VALUE kpve
      join KPI_PARAM kpm 
      on kpm.kpi_param_id = kpve.kpi_param_id 
        and kpm.status = 1
      where kpve.kpi_config_id = i_kpi_config_id
        and kpve.status in (0, 1)
        and kpm.type = 11
        and kpve.FROM_KPI_PERIOD_VALUE <= i_kpi_period_value 
        and (kpve.TO_KPI_PERIOD_VALUE >= i_kpi_period_value or kpve.TO_KPI_PERIOD_VALUE is null);
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'not found KPI_GROUP_CONFIG_ID');
      return;
    when too_many_rows then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'too many KPI_GROUP_CONFIG_ID');
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get KPI_GROUP_CONFIG_ID|exception: ' 
          || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 500));
      return;
    end;
    
    v_sql := 
      '-- ds NV tinh KPI
      with sf_tmp as (
        select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
          , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
        from STAFF sf 
        join STAFF_TYPE_TMP ste 
        on ste.staff_id = sf.staff_id
        where sf.status = 1 ' ||
          (case when i_object_type = 2 then 
            ' and sf.staff_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
            ' and ste.staff_type_id = ' || i_object_id || ' '
          end)
      || ' )
      , sp_tmp as (
          select distinct sf_tmp.istaff_id as staff_id, musp.inherit_shop_id as shop_id
          from sf_tmp
          join MAP_USER_SHOP musp
          on sf_tmp.istaff_id = musp.user_id
          where musp.status in (0, 1)
              and musp.from_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
              and (musp.to_date >= to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musp.to_date is null)
      )
      , dta_tmp as (
        select sp_tmp.staff_id, sp_tmp.shop_id, kgcg.kpi_group_config_id, kqta.kpi_config_id
          , sum(nvl(kqta.score, 0)) as tt_score
        from KPI_GROUP_CONFIG kgcg
        join KPI_REGISTER krr
        on kgcg.kpi_group_config_id = krr.kpi_group_config_id
          and krr.status = 1 and krr.kpi_period = 1
          and krr.from_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
          and (krr.to_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or krr.to_date is null)
        join KPI_QUOTA kqta
        on krr.kpi_register_id = kqta.kpi_register_id
          and kqta.status = 1 and kqta.object_type = 1
          and kqta.weighted is not null
          and kqta.kpi_period_value = ' || i_kpi_period_value || '
          and exists (
            select 1
            from KPI_CONFIG kcg
            where kqta.kpi_config_id = kcg.kpi_config_id
              and kcg.status = 1
              and kcg.update_type = 1 -- tay
          )
          and kqta.score is not null -- chi lay khi da cham diem
        join sp_tmp
        on kqta.object_id = sp_tmp.shop_id
        where kgcg.kpi_group_config_id = ' || v_kpi_group_id || '
          and kgcg.status = 1
        group by sp_tmp.staff_id, sp_tmp.shop_id, kgcg.kpi_group_config_id, kqta.kpi_config_id
      )
      select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , kqa.plan_value as plan_value
          , dta.avg_score as gain
      from KPI_REGISTER_HIS vkrr
      join sf_tmp sf 
      on 1 = 1 ' ||
      (case when i_object_type = 2 then 
              ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
              ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                and sf.istaff_id not in (
                      select krhs.object_id
                      from KPI_REGISTER_HIS krhs
                      where krhs.kpi_period = 1
                        and krhs.kpi_period_value = vkrr.kpi_period_value
                        and krhs.object_type = 2 -- NV
                        and krhs.kpi_group_config_id is not null)'
       end) ||
      ' join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.status         = 1
              and kqat.weighted is not null
              and kqat.object_type    = 2
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
      left join (
        select staff_id, avg(tt_score) as avg_score
        from dta_tmp
        group by staff_id
      ) dta
      on sf.istaff_id = dta.staff_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type || '
        and kqa.plan_value is not null ';

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          -- t�nh ?i?m cho t?ng ti�u ch� KPI trong b?.
          /*if v_dta(indx).plan_value is null 
              or (v_dta(indx).plan_value <= 0 and v_dta(indx).gain is null )
          then
            v_score := 0;
          elsif v_dta(indx).plan_value <= 0 then
              v_score := round(((1 * nvl(v_dta(indx).weighted, 100))/ 100), 2);
          else 
            if i_max_value is not null and v_dta(indx).gain > i_max_value then
              v_score := round((((i_max_value / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            else 
              v_score := round((((v_dta(indx).gain / v_dta(indx).plan_value) * nvl(v_dta(indx).weighted, 100))/ 100), 2);
            end if;
          end if;*/
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_AVG_KPI_SCORE_SHOP_CYCLE;
  
  PROCEDURE P_KPI_STAFF_OFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date  date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [KPI ?n ??nh nh�n s?; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n c? th?; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql         clob; 
    v_kpi_period  number;
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_score       RPT_KPI_CYCLE.SCORE%TYPE;
    v_cyc_bdate   date;
    v_cyc_edate   date;
    vv_cdate      date;
    vv_specific_type  STAFF_TYPE.specific_type%TYPE;
    vv_prefix         STAFF_TYPE.prefix%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_STAFF_OFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type is null
        or i_object_id is null
        or v_kpi_period is null
        or i_kpi_period_value is null
        or i_kpi_group_config_id is null
        or i_kpi_config_id is null
        or i_kpi_config_code is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    begin
      select ce.begin_date, ce.end_date
      into v_cyc_bdate, v_cyc_edate
      from CYCLE ce
      where cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'cycle id not found');
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle|exception: ' 
        || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 200));
      return;
    end;
    
    if v_cyc_bdate is null or v_cyc_edate is null then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'begin date or end date of cycle is null');
      return;
    end if;
    
    -- lay ngay cuoi cung, trong truong hop chay tong hop KPI cho ky chua ket thuc
    if trunc(v_cyc_edate) > trunc(i_input_date) then
      vv_cdate := trunc(i_input_date);
    else 
      vv_cdate := trunc(v_cyc_edate);
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2, 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type, ste.prefix
      into vv_specific_type, vv_prefix
      from STAFF sf
      join STAFF_TYPE_TMP ste
      on sf.staff_id = ste.staff_id
      where sf.status in (0, 1)
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type, ste.prefix
      into vv_specific_type, vv_prefix
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [2: GSNPP; 3: tren GSNPP]
    if vv_specific_type not in (2, 3) 
        or vv_prefix not in ('GDM', 'GDV', 'GDKV', 'GS') then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [2, 3] or vv_prefix not in [GDM, GDV, GDKV, GS]');
      return;
    end if;
    
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    v_sql := 
      -- ds NV tinh KPI
     'with sf_tmp as (
        select distinct sf.staff_id as istaff_id, sf.shop_id as ishop_id
          , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
        from STAFF sf 
        join STAFF_TYPE_TMP ste 
        on ste.staff_id = sf.staff_id
        where sf.status in (0, 1) ' ||
          (case when i_object_type = 2 then 
            ' and sf.staff_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
            ' and ste.staff_type_id = ' || i_object_id || ' '
          end)
      || ' )
      , isf_tmp as (
          select sf_tmp.istaff_id
            , sf.staff_id, sf.status
            , ste.specific_type
            , ( case when sh.status = 0 
                      and exists (
                        select 1
                        from STAFF_HISTORY shh
                        where 1 = 1
                          -- and shh.staff_history_id = sh.pre_id
                          and sf.staff_id = shh.staff_id
                          and shh.status = 1
                          and shh.from_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                          and (shh.to_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') 
                            or shh.to_date is null))
                  then 1
                  else 0 end) as is_off
            , ( case when sh.status = 1
                      and musf.from_date < to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                      and (musf.to_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') 
                        or musf.to_date is null)
                  then 1
                  else 0
                end) as is_active_first
            , ( case when sh.status = 1
                      and musf.from_date < to_date(''' || to_char(vv_cdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
                      and (musf.to_date >= to_date(''' || to_char(vv_cdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') 
                        or musf.to_date is null)
                  then 1
                  else 0
                end) as is_active_last
            , row_number() over (
                  partition by sf_tmp.istaff_id, sf.staff_id 
                  order by sh.action_date desc) as rn
          from sf_tmp
          join MAP_USER_STAFF musf
          on musf.user_id = sf_tmp.istaff_id
            and musf.status in (0, 1)
            and musf.from_date < to_date(''' || to_char(v_cyc_edate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
            and (musf.to_date >= to_date(''' || to_char(v_cyc_bdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') or musf.to_date is null)
          join STAFF sf
          on musf.inherit_staff_id = sf.staff_id
            and sf.status in (0, 1)
          join STAFF_TYPE ste
          on ste.staff_type_id = sf.staff_type_id
            and ste.status = 1
            and ste.specific_type 
            ' ||(case when vv_specific_type = 2 and vv_prefix in ('GS') then 
                    ' in (1) '
                  when vv_specific_type = 3 and vv_prefix in ('GDV', 'GDKV') then 
                    ' in (1, 2) '
                  when vv_specific_type = 3 and vv_prefix in ('GDM') then -- ko tinh RSM
                    ' in (1, 2, 3) and ste.prefix in (''GDV'', ''GDKV'', ''GS'', ''NV'')'
                end) || '
          left join STAFF_HISTORY sh
          on sh.status in (0, 1)
            and sh.staff_id = sf.staff_id
            and sh.from_date  < to_date(''' || to_char(vv_cdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
            and (sh.to_date  >= to_date(''' || to_char(vv_cdate, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'')
              or sh.to_date is null)
      )
      , isf_tmp2 as (
        select istaff_id
          , (   count(distinct (case when is_active_last  = 1 and specific_type  = 1 then staff_id else null end)) 
              + count(distinct (case when is_active_first = 1 and specific_type  = 1 then staff_id else null end)) 
            ) / 2 as avg_nv -- trung binh NV
          , (   count(distinct (case when is_active_last  = 1 and specific_type != 1 then staff_id else null end))
              + count(distinct (case when is_active_first = 1 and specific_type != 1 then staff_id else null end))
            ) / 2 as avg_gs -- trung binh GS
          , count(distinct (case when is_off = 1 and specific_type  = 1 then staff_id else null end)) as tt_off_nv
          , count(distinct (case when is_off = 1 and specific_type != 1 then staff_id else null end)) as tt_off_gs
        from isf_tmp
        where rn = 1
        group by istaff_id
      )
      , isf_tmp3 as (
        select istaff_id
          , avg_nv
          , avg_gs
          , tt_off_nv
          , tt_off_gs
          , round(avg_nv / nullif(avg_gs, 0), 2) as iidex
        from isf_tmp2
      )
      , dta_tmp as (
        select istaff_id as staff_id
          , ' || (case when vv_prefix in ('GS') then 
                      '(tt_off_nv)'
                    when vv_prefix in ('GDM', 'GDV', 'GDKV') then 
                      '(tt_off_nv + tt_off_gs) '
                  end) || ' as tt_of_staff
          , ' || (case when vv_prefix in ('GS') then 
                      '(avg_nv) '
                    when vv_prefix in ('GDM', 'GDV', 'GDKV') then 
                      '(avg_nv + avg_gs) '
                  end) || ' as tt_avg_staff
          , ' || (case when vv_prefix in ('GS') then 
                      '(tt_off_nv) * 100/ nullif(avg_nv, 0) '
                    when vv_prefix in ('GDM', 'GDV', 'GDKV') then 
                      '(tt_off_nv + tt_off_gs * iidex) * 100 / nullif(avg_nv * avg_gs * iidex, 0) '
                  end) || ' as gain
        from isf_tmp3
      )
      select sf.istaff_id as staff_id, sf.ishop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , kqa.plan_value as plan_value
          , (case when dta_tmp.tt_of_staff = 0 then -1
                  when dta_tmp.tt_of_staff > 0 and dta_tmp.tt_of_staff > dta_tmp.tt_avg_staff then -2
                  when dta_tmp.gain < 0 then 0 else round(dta_tmp.gain, 2) 
             end) as gain
      from KPI_REGISTER_HIS vkrr
      join sf_tmp sf 
      on 1 = 1 ' ||
      (case when i_object_type = 2 then 
              ' and sf.istaff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
              ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                and sf.istaff_id not in (
                  select krhs.object_id
                  from KPI_REGISTER_HIS krhs
                  where krhs.kpi_period = 1
                    and krhs.kpi_period_value = vkrr.kpi_period_value
                    and krhs.object_type = 2 -- NV
                    and krhs.kpi_group_config_id is not null)'
       end) ||
      ' join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.status         = 1
              and kqat.weighted is not null
              and kqat.object_type    = 2
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sf.istaff_id and kqa.kpi_config_id = vkrr.kpi_config_id
      left join dta_tmp
      on sf.istaff_id = dta_tmp.staff_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type || '
        and kqa.plan_value is not null ';

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          if v_dta(indx).gain = -1 then
            v_dta(indx).gain := 0;
            v_score := i_max_value;
          elsif v_dta(indx).gain = -2 then
            v_dta(indx).gain := 0;
            v_score := 0;
          else
            v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
                v_dta(indx).plan_value
              , v_dta(indx).gain
              , v_dta(indx).weighted
              , i_max_value
            );
          end if;
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_STAFF_OFF_CYCLE;
  
  PROCEDURE P_KPI_AVG_STOCK_SHOP_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [T?n kho t?i thi?u NPP; NPP; chu k?];
    @author: thuattq1
    
    @params:  
      i_object_type         : Lo?i ??i t??ng: 1: NPP; 3: lo?i NPP.
      i_object_id           : ID nh�n vi�n/lo?i nv.
      i_kpi_period_value    : ID gi� tr? k?.
      i_kpi_group_config_id : ID nh�m KPI.
      i_kpi_config_id       : ID KPI.
      i_kpi_config_code     : M� KPI.
      i_plan_type           : lo?i ph�n b?: 1: ko ph�n b?; 2: c� ph�n b?.
      i_kpi_reg_id          : ID KPI_REGISTER.
      i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob;
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_params  nvarchar2(2000);
    v_score   RPT_KPI_CYCLE.SCORE%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_AVG_STOCK_SHOP_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      shop_id         SHOP.shop_id%type,
      weighted        KPI_GROUP_DETAIL.weighted%type,
      max_value       KPI_GROUP_DETAIL.max_value%type,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.plan%type,
      gain            RPT_KPI_CYCLE.done%type,
      gain_ir         RPT_KPI_CYCLE.done_ir%type,
      gain_or         RPT_KPI_CYCLE.done_or%type
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    if  i_object_type   is null
        or i_object_id  is null
        or i_kpi_period_value     is null
        or i_kpi_group_config_id  is null
        or i_kpi_config_id    is null
        or i_kpi_config_code  is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    if i_object_type not in (1, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [1; 3]');
      return;
    end if;
    
    v_sql := 
      ---- ds NV tinh KPI
     'with ce_tmp as (
        select ce.cycle_id, trunc(ce.begin_date) as begin_date, trunc(ce.end_date) as end_date
        from CYCLE ce
        where ce.cycle_id = ' || i_kpi_period_value || '
      )
      , sp_tmp as (
        select sp.shop_id as shop_id
          , ste.specific_type as specific_type, ste.shop_type_id as shop_type_id
          , ce_tmp.cycle_id, ce_tmp.begin_date, ce_tmp.end_date
          , F_WORKINGS_DAYS_CUMULATE (ce_tmp.begin_date, ce_tmp.end_date
              , ''NULL'', sp.shop_id, 0) as cycle_working_day
          '|| --[ko su dung WORKING_DAY do chi lay ngay hien tai]
         ', spt.product_id
          , min(spt.minsf) as minsf, max(spt.maxsf) as maxsf
        from SHOP sp
        join SHOP_TYPE ste
        on sp.shop_type_id = ste.shop_type_id
          and ste.status = 1
        join ce_tmp on 1 = 1 
        left join SHOP_PRODUCT spt
        on sp.shop_id = spt.shop_id
          and spt.status = 1 and spt.type = 2
        where sp.status = 1 ' ||
          (case when i_object_type = 1 then 
                  ' and sp.shop_id = ' || i_object_id || ' '
                when i_object_type = 3 then 
                  ' and ste.shop_type_id = ' || i_object_id || ' '
           end)|| ' 
        group by sp.shop_id, ste.specific_type, ste.shop_type_id
          , ce_tmp.cycle_id, ce_tmp.begin_date, ce_tmp.end_date--, wdy.cycle_working_day
          , spt.product_id
      ) '||
      -- lay thong tin san pham; ko lay POSM
     ', pt_tmp as (
        select pt.product_id, pt.convfact as convfact
        from PRODUCT pt
        left join PRODUCT_INFO pio
        on pt.product_type = pio.product_info_id
          and pio.status = 1
        where pt.status = 1
          and nvl(pio.type, -1) != 7
      )'||
      -- lay ke hoach doanh so ban NPP
     ', spn_tmp as (
        select sp_tmp.shop_id
          , sp_tmp.cycle_working_day
          , sp_tmp.product_id
          , sp_tmp.minsf * round(sum(nvl(spn.amount, 0))
              /nullif(sp_tmp.cycle_working_day, 0), 2) as amount_min 
          , sp_tmp.maxsf * round(sum(nvl(spn.amount, 0))
              /nullif(sp_tmp.cycle_working_day, 0), 2) as amount_max
        from sp_tmp
        left join SALE_PLAN spn
        on sp_tmp.shop_id = spn.object_id and spn.object_type = 3
          and spn.type = 3
          and spn.cycle_id = sp_tmp.cycle_id
          and spn.product_id = sp_tmp.product_id
          and spn.amount is not null 
        join pt_tmp
        on spn.product_id = pt_tmp.product_id
        group by sp_tmp.shop_id
          , sp_tmp.cycle_working_day, sp_tmp.minsf, sp_tmp.maxsf
          , sp_tmp.product_id
      )'||
      -- ton kho nha phan phoi trong chu ky
     ', sp_stock_tmp as (
        select sp_tmp.shop_id, sp_tmp.cycle_working_day, pt_tmp.product_id, trunc(rpt.rpt_in_day) as stock_date
          --, sum(nvl(rpt.current_stock, 0)) as total
          , nvl(trunc(sum(nvl(rpt.current_stock, 0))/nullif(pt_tmp.convfact, 0)), 0) as tt_package
          , nvl(mod(sum(nvl(rpt.current_stock, 0)), nvl(pt_tmp.convfact, 0)), 0) as tt_retail
        from sp_tmp
        left join RPT_WAREHOUSE_DAY rpt
        on sp_tmp.shop_id = rpt.shop_id
          and sp_tmp.product_id = rpt.product_id
          and rpt.rpt_in_day >= sp_tmp.begin_date
          and rpt.rpt_in_day <  sp_tmp.end_date + 1
          and exists (
            select 1
            from WAREHOUSE we
            where we.status = 1 and we.warehouse_id = rpt.warehouse_id
          )
        join pt_tmp
        on rpt.product_id = pt_tmp.product_id
        group by sp_tmp.shop_id, sp_tmp.cycle_working_day, pt_tmp.product_id, pt_tmp.convfact
          , trunc(rpt.rpt_in_day)
      )'||
      -- lay gia tri hang ton kho
     ', sp_amount_tmp as (
        select sp_stock_tmp.shop_id, sp_stock_tmp.cycle_working_day, sp_stock_tmp.product_id
          , sp_stock_tmp.stock_date
          -- , sum(nvl(sp_stock_tmp.total * pe.price, 0)) as s_total
          , sum(nvl(sp_stock_tmp.tt_package * pe.package_price, 0) 
        + nvl(sp_stock_tmp.tt_retail * pe.price, 0)) as s_total
        from sp_stock_tmp 
        left join PRICE_SHOP_DEDUCED pe
        on pe.shop_id = sp_stock_tmp.shop_id 
          and pe.product_id = sp_stock_tmp.product_id
          and pe.status = 1
          and pe.from_date < sp_stock_tmp.stock_date + 1
          and (pe.to_date >= sp_stock_tmp.stock_date or pe.to_date is null) 
        group by sp_stock_tmp.shop_id, sp_stock_tmp.cycle_working_day, sp_stock_tmp.product_id
          , sp_stock_tmp.stock_date
      )
      , dta_tmp as ( ' ||
      
        /*select nvl(spn_tmp.shop_id, sp_amount_tmp.shop_id) as shop_id
          , spn_tmp.cycle_working_day
          , spn_tmp.amount_min, spn_tmp.amount_max
          , sp_amount_tmp.stock_date
          , sp_amount_tmp.s_total
          , (case when s_total between spn_tmp.amount_min and spn_tmp.amount_max then 1 else 0 end) as result
        from spn_tmp
        full join sp_amount_tmp
        on spn_tmp.shop_id = sp_amount_tmp.shop_id
        order by spn_tmp.shop_id, sp_amount_tmp.stock_date*/
        
        --, count(1) * 100/nullif(spn_tmp.cycle_working_day, 0) as gain
        --having s_total between spn_tmp.amount_min and spn_tmp.amount_max
        
        '
        select nvl(spn_tmp.shop_id, sp_amount_tmp.shop_id) as shop_id
          , nvl(spn_tmp.product_id, sp_amount_tmp.product_id) as product_id
          , nvl(spn_tmp.cycle_working_day, sp_amount_tmp.cycle_working_day) as cycle_working_day
          , spn_tmp.amount_min, spn_tmp.amount_max
          , sp_amount_tmp.stock_date
          , sp_amount_tmp.s_total
          , (case when not(s_total between spn_tmp.amount_min and spn_tmp.amount_max) then 1 else 0 end) as is_invalidate
          -- vi pham ton kho
        from spn_tmp
        left join sp_amount_tmp
        on spn_tmp.shop_id = sp_amount_tmp.shop_id
          and spn_tmp.product_id = sp_amount_tmp.product_id
      )
      , dta2_tmp as (
        select shop_id, cycle_working_day, stock_date
          , count(distinct product_id ) as tt_product
          , count(distinct (case when is_invalidate = 1 then product_id else null end)) as tt_invalidate 
        from dta_tmp
        where shop_id is not null and product_id is not null
        group by shop_id, cycle_working_day, stock_date
      )
      select sp.shop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , kqa.plan_value as plan_value
          , round(dta.gain, 2) as gain
          , null as gain_ir
          , null as gain_or
      from KPI_REGISTER_HIS vkrr
      join (
        select distinct shop_id, specific_type, shop_type_id
        from sp_tmp 
      ) sp 
      on 1 = 1 ' ||
      (case when i_object_type = 1 then 
              ' and sp.shop_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
            when i_object_type = 3 then 
              ' and sp.shop_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                and sp.shop_id not in (
                  select krhs.object_id
                  from KPI_REGISTER_HIS krhs
                  where krhs.kpi_period = 1
                    and krhs.kpi_period_value = vkrr.kpi_period_value
                    and krhs.object_type = 1 -- NPP
                    and krhs.kpi_group_config_id is not null)'
       end) || ' 
      join (
         select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
         from KPI_QUOTA kqat
         where kqat.kpi_period_value = ' || i_kpi_period_value || '
             and kqat.object_type = 1 -- NPP
             and kqat.status = 1
             and kqat.weighted is not null
             and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
             and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sp.shop_id
      left join (
        select shop_id
          , sum(case when tt_invalidate = 0 and tt_product > 0 then 1 else 0 end) * 100
            /nullif(cycle_working_day, 0) as gain
        from dta2_tmp
        group by shop_id, cycle_working_day
      ) dta
      on sp.shop_id = dta.shop_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type      
        /*|| (case when i_plan_type = 2 then 'and kqa.plan_value is not null ' 
              when i_plan_type = 1 then 'and spn.plan is not null ' 
              else null end)*/
    ;

    --dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 3 -- nhan vien
              and rpt.object_id = v_dta(indx).shop_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   done_ir             = v_dta(indx).gain_ir,
                   done_or             = v_dta(indx).gain_or,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, done_ir, 
               done_or, score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             3,
             v_dta(indx).shop_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_dta(indx).gain_ir,
             v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, done_ir, 
               done_or, score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             3,
             v_dta(indx).shop_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_dta(indx).gain_ir,
             v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_AVG_STOCK_SHOP_CYCLE;
  
  PROCEDURE P_KPI_SUCC_ORDER_SHOP_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [T? l? giao h�ng th�nh c�ng; NPP; chu k?];
    @author: thuattq1
    
    @params:  
      i_object_type         : Lo?i ??i t??ng: 1: NPP; 3: lo?i NPP.
      i_object_id           : ID nh�n vi�n/lo?i nv.
      i_kpi_period_value    : ID gi� tr? k?.
      i_kpi_group_config_id : ID nh�m KPI.
      i_kpi_config_id       : ID KPI.
      i_kpi_config_code     : M� KPI.
      i_plan_type           : lo?i ph�n b?: 1: ko ph�n b?; 2: c� ph�n b?.
      i_kpi_reg_id          : ID KPI_REGISTER.
      i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob;
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_params  nvarchar2(2000);
    v_score   RPT_KPI_CYCLE.SCORE%TYPE;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_SUCC_ORDER_SHOP_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      shop_id         SHOP.shop_id%type,
      weighted        KPI_GROUP_DETAIL.weighted%type,
      max_value       KPI_GROUP_DETAIL.max_value%type,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.plan%type,
      gain            RPT_KPI_CYCLE.done%type,
      gain_ir         RPT_KPI_CYCLE.done_ir%type,
      gain_or         RPT_KPI_CYCLE.done_or%type
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    if  i_object_type   is null
        or i_object_id  is null
        or i_kpi_period_value     is null
        or i_kpi_group_config_id  is null
        or i_kpi_config_id    is null
        or i_kpi_config_code  is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    if i_object_type not in (1, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [1; 3]');
      return;
    end if;
    
    v_sql := 
      ---- ds NV tinh KPI
      'with sp_tmp as (
        select sp.shop_id as shop_id
          , ste.specific_type as specific_type, ste.shop_type_id as shop_type_id
          , ce.cycle_id, ce.begin_date, ce.end_date
        from SHOP sp
        join SHOP_TYPE ste
        on sp.shop_type_id = ste.shop_type_id
          and ste.status = 1
        join CYCLE ce on ce.cycle_id = ' || i_kpi_period_value || '
        where sp.status = 1 ' ||
          (case when i_object_type = 1 then 
                  ' and sp.shop_id = ' || i_object_id || ' '
                when i_object_type = 3 then 
                  ' and ste.shop_type_id = ' || i_object_id || ' '
           end)|| ' 
        group by sp.shop_id, ste.specific_type, ste.shop_type_id
          , ce.cycle_id, ce.begin_date, ce.end_date
      )
      , dta_tmp as (
        select sp_tmp.shop_id
          , count(case when sor.type = 2 then 1 else null end) as tt_return 
          , count(case when sor.type in (0, 1) then 1 else null end) as tt_order
        from sp_tmp
        join SALE_ORDER sor
        on sor.shop_id = sp_tmp.shop_id
          and sor.cycle_id = sp_tmp.cycle_id
          and sor.order_date >= sp_tmp.begin_date
          and sor.order_date <  sp_tmp.end_date + 1
          and sor.approved = 1
          and sor.type in (0, 1, 2)
          and nvl(sor.amount, 0) > 0 
        group by sp_tmp.shop_id
      )
      select sp.shop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , kqa.plan_value as plan_value
          , round((dta_tmp.tt_order - dta_tmp.tt_return) * 100
              / nullif(dta_tmp.tt_order, 0), 2) as gain
          , null as gain_ir
          , null as gain_or
      from KPI_REGISTER_HIS vkrr
      join sp_tmp sp 
      on 1 = 1 ' ||
      (case when i_object_type = 1 then 
              ' and sp.shop_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
            when i_object_type = 3 then 
              ' and sp.shop_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                and sp.shop_id not in (
                  select krhs.object_id
                  from KPI_REGISTER_HIS krhs
                  where krhs.kpi_period = 1
                    and krhs.kpi_period_value = vkrr.kpi_period_value
                    and krhs.object_type = 1 -- NPP
                    and krhs.kpi_group_config_id is not null)'
       end) || ' 
      join (
         select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
         from KPI_QUOTA kqat
         where kqat.kpi_period_value = ' || i_kpi_period_value || '
             and kqat.object_type = 1 -- NPP
             and kqat.status = 1
             and kqat.weighted is not null
             and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
             and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sp.shop_id
      left join dta_tmp 
      on sp.shop_id = dta_tmp.shop_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type      
        /*|| (case when i_plan_type = 2 then 'and kqa.plan_value is not null ' 
              when i_plan_type = 1 then 'and spn.plan is not null ' 
              else null end)*/
    ;

    --dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 3 -- nhan vien
              and rpt.object_id = v_dta(indx).shop_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   done_ir             = v_dta(indx).gain_ir,
                   done_or             = v_dta(indx).gain_or,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, done_ir, 
               done_or, score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             3,
             v_dta(indx).shop_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_dta(indx).gain_ir,
             v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, done_ir, 
               done_or, score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             3,
             v_dta(indx).shop_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_dta(indx).gain_ir,
             v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_SUCC_ORDER_SHOP_CYCLE;
  
  PROCEDURE P_KPI_PRO_RETURN_SHOP_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [H�ng tr? v?; NPP; chu k?];
    @author: thuattq1
    
    @params:  
      i_object_type         : Lo?i ??i t??ng: 1: NPP; 3: lo?i NPP.
      i_object_id           : ID nh�n vi�n/lo?i nv.
      i_kpi_period_value    : ID gi� tr? k?.
      i_kpi_group_config_id : ID nh�m KPI.
      i_kpi_config_id       : ID KPI.
      i_kpi_config_code     : M� KPI.
      i_plan_type           : lo?i ph�n b?: 1: ko ph�n b?; 2: c� ph�n b?.
      i_kpi_reg_id          : ID KPI_REGISTER.
      i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob;
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_params  nvarchar2(2000);
    v_score   RPT_KPI_CYCLE.SCORE%TYPE;
    
    v_ncycle        number := 5; -- 5 thang + thang hien tai
    v_ncycle_bdate  date;
    v_ncycle_id     cycle.cycle_id%type;
    v_cyc_bdate     date;
    v_cyc_edate     date;
    
    v_pro_name      constant varchar2(200) := 'P_KPI_PRO_RETURN_SHOP_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      shop_id         SHOP.shop_id%type,
      weighted        KPI_GROUP_DETAIL.weighted%type,
      max_value       KPI_GROUP_DETAIL.max_value%type,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.plan%type,
      gain            RPT_KPI_CYCLE.done%type,
      gain_ir         RPT_KPI_CYCLE.done_ir%type,
      gain_or         RPT_KPI_CYCLE.done_or%type
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    if  i_object_type   is null
        or i_object_id  is null
        or i_kpi_period_value     is null
        or i_kpi_group_config_id  is null
        or i_kpi_config_id    is null
        or i_kpi_config_code  is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    if i_object_type not in (1, 3) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [1; 3]');
      return;
    end if;
    
    begin
      select trunc(ce.begin_date), trunc(ce.end_date)
      into v_cyc_bdate, v_cyc_edate
      from CYCLE ce
      where cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'cycle id not found');
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle|exception: ' 
        || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 200));
      return;
    end;
    
    -- lay thong tin n chu ky truoc
    begin
      v_ncycle_id := F_GET_CYCLE_SEED_BY_CYCLE(i_kpi_period_value, -1 * v_ncycle);
      
      select trunc(ce.begin_date)
      into v_ncycle_bdate
      from CYCLE ce
      where cycle_id = v_ncycle_id;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'ncycle not found');
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get ncycle|exception: ' 
        || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 200));
      return;
    end;
    
    /*
      join AP_PARAM apm
      on horodl.recall_reason_id = apm.ap_param_id
        and apm.ap_param_code in (C_RREASON_HET_DATE, C_RREASON_CAN_DATE, C_RREASON_PHONG_XI)
    */
    
    v_sql := 
      ---- ds NV tinh KPI
      'with sp_tmp as (
        select sp.shop_id as shop_id
          , ste.specific_type as specific_type, ste.shop_type_id as shop_type_id
          , ce.cycle_id, ce.begin_date, ce.end_date
        from SHOP sp
        join SHOP_TYPE ste
        on sp.shop_type_id = ste.shop_type_id
          and ste.status = 1
        join CYCLE ce on ce.cycle_id = ' || i_kpi_period_value || '
        where sp.status = 1 ' ||
          (case when i_object_type = 1 then 
                  ' and sp.shop_id = ' || i_object_id || ' '
                when i_object_type = 3 then 
                  ' and ste.shop_type_id = ' || i_object_id || ' '
           end)|| ' 
        group by sp.shop_id, ste.specific_type, ste.shop_type_id
          , ce.cycle_id, ce.begin_date, ce.end_date
      )
      , cr_tmp as (
        select sp_tmp.shop_id, cr.customer_id
        from sp_tmp
        join CUSTOMER_SHOP_MAP csmp
        on csmp.shop_id = sp_tmp.shop_id
          and csmp.from_date < to_date('''||to_char(v_cyc_edate, 'yyyymmdd')||''', ''yyyymmdd'') + 1
          and (csmp.to_date >= to_date('''||to_char(v_cyc_bdate, 'yyyymmdd')||''', ''yyyymmdd'') 
            or csmp.to_date is null)
        join CUSTOMER cr
        on csmp.customer_id = cr.customer_id
      )
      , return_tmp as (
        select horor.shop_id
          , sum(nvl(horodl.quantity * pt.volumn, 0)) as return_qtt
        from HO_RETURN_ORDER horor
        join HO_RETURN_ORDER_DETAIL horodl
        on horor.ho_return_order_id = horodl.ho_return_order_id
        join PRODUCT pt
        on horodl.product_id = pt.product_id
          and pt.status in (0, 1)
        where horor.status in (1)
          and horor.return_date >= to_date('''||to_char(v_cyc_bdate, 'yyyymmdd')||''', ''yyyymmdd'')
          and horor.return_date <  to_date('''||to_char(v_cyc_edate, 'yyyymmdd')||''', ''yyyymmdd'') + 1
          and horodl.return_date >= to_date('''||to_char(v_cyc_bdate, 'yyyymmdd')||''', ''yyyymmdd'')
          and horodl.return_date <  to_date('''||to_char(v_cyc_edate, 'yyyymmdd')||''', ''yyyymmdd'') + 1
          and horor.shop_id in (
            select shop_id
            from sp_tmp
          )
        group by horor.shop_id
      )
      , order_tmp as (
        select cr_tmp.shop_id
          , sum(nvl(pt.volumn * (case when sor.type = 2 then -1 else 1 end) * (case when sodl.is_free_item = 0 then sodl.quantity else 0 end), 0)) as sale_qtt
          , sum(nvl(pt.volumn * (case when sor.type = 2 then -1 else 1 end) * (case when sodl.is_free_item = 1 and sodl.program_type in (0, 1, 3, 4, 5, 7, 8) then sodl.quantity else 0 end), 0)) as promo_qtt
          , sum(nvl(pt.volumn * (case when sor.type = 2 then -1 else 1 end) * (case when sodl.is_free_item = 1 and sodl.program_type in (2, 6) then sodl.quantity else 0 end), 0)) as ks_qtt
        from cr_tmp
        join SALE_ORDER sor
        on cr_tmp.customer_id = sor.customer_id
        join SALE_ORDER_DETAIL sodl
        on sor.sale_order_id = sodl.sale_order_id
        join PRODUCT pt
        on sodl.product_id = pt.product_id
          and pt.status in (0, 1)
        where sor.approved = 1 and sor.amount > 0
          and sor.type in (0, 1, 2) -- (tong ban - tong tra)
          and sor.order_date >= to_date('''||to_char(v_ncycle_bdate, 'yyyymmdd')||''', ''yyyymmdd'')
          and sor.order_date <  to_date('''||to_char(v_cyc_edate, 'yyyymmdd')||''', ''yyyymmdd'') + 1
          and sodl.order_date >= to_date('''||to_char(v_ncycle_bdate, 'yyyymmdd')||''', ''yyyymmdd'')
          and sodl.order_date <  to_date('''||to_char(v_cyc_edate, 'yyyymmdd')||''', ''yyyymmdd'') + 1
          --and sor.shop_id in ( select shop_id from sp_tmp)
        group by cr_tmp.shop_id
      )
      , dta_tmp as (
        select sp_tmp.shop_id
          , sum(nvl(return_tmp.return_qtt, 0)) as return_qtt
          , round(sum(nvl(order_tmp.sale_qtt + order_tmp.promo_qtt + order_tmp.ks_qtt, 0))/6, 2) as order_qtt
        from sp_tmp
        left join return_tmp
        on sp_tmp.shop_id = return_tmp.shop_id
        left join order_tmp
        on sp_tmp.shop_id = order_tmp.shop_id
        group by sp_tmp.shop_id
      )
      select sp.shop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , kqa.plan_value as plan_value
          , round((dta_tmp.return_qtt) * 1000
              / nullif(dta_tmp.order_qtt, 0), 2) as gain
          , null as gain_ir
          , null as gain_or
      from KPI_REGISTER_HIS vkrr
      join sp_tmp sp 
      on 1 = 1 ' ||
      (case when i_object_type = 1 then 
              ' and sp.shop_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
            when i_object_type = 3 then 
              ' and sp.shop_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                and sp.shop_id not in (
                  select krhs.object_id
                  from KPI_REGISTER_HIS krhs
                  where krhs.kpi_period = 1
                    and krhs.kpi_period_value = vkrr.kpi_period_value
                    and krhs.object_type = 1 -- NPP
                    and krhs.kpi_group_config_id is not null)'
       end) || ' 
      join (
         select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
         from KPI_QUOTA kqat
         where kqat.kpi_period_value = ' || i_kpi_period_value || '
             and kqat.object_type = 1 -- NPP
             and kqat.status = 1
             and kqat.weighted is not null
             and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
             and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sp.shop_id
      left join dta_tmp 
      on sp.shop_id = dta_tmp.shop_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type      
        /*|| (case when i_plan_type = 2 then 'and kqa.plan_value is not null ' 
              when i_plan_type = 1 then 'and spn.plan is not null ' 
              else null end)*/
    ;

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 3 -- nhan vien
              and rpt.object_id = v_dta(indx).shop_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          v_score := PKG_KPI_YEAR.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   done_ir             = v_dta(indx).gain_ir,
                   done_or             = v_dta(indx).gain_or,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, done_ir, 
               done_or, score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             3,
             v_dta(indx).shop_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_dta(indx).gain_ir,
             v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, done_ir, 
               done_or, score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             3,
             v_dta(indx).shop_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_dta(indx).gain_ir,
             v_dta(indx).gain_or, 
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));
  end P_KPI_PRO_RETURN_SHOP_CYCLE;
  
  PROCEDURE P_RUN_KPI_CYCLE (
      i_input_date date default trunc(sysdate)
    , i_update_kpi_register number default 1
  )
  as
    v_pro_name      constant varchar2(200) := 'P_RUN_KPI_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    -- v_cycle_id CYCLE.CYCLE_ID%TYPE;
    
    vv_run_date date;
    vv_kpi_date CYCLE.end_date%type;
    v_cycle_id CYCLE.CYCLE_ID%TYPE;
    vv_bccycle_date CYCLE.end_date%type; -- ngay dau chu ky hien tai
    vv_eccycle_date CYCLE.end_date%type; -- ngay cuoi chu ky hien tai
    
    -- lay danh sach cac nhom KPI + object dang ky.
    cursor c_kpi_group is
    select vkrr.kpi_register_id, vkrr.kpi_group_config_id
      , vkrr.kpi_group_code as group_code
      , vkrr.object_type, vkrr.object_id
    from KPI_REGISTER_HIS vkrr
    where vkrr.kpi_period = 1 -- chu ky
      and vkrr.kpi_period_value = v_cycle_id
    group by vkrr.kpi_register_id, vkrr.kpi_group_config_id
      , vkrr.kpi_group_code
      , vkrr.object_type, vkrr.object_id
    order by vkrr.kpi_group_config_id, vkrr.kpi_register_id
      , vkrr.object_type, vkrr.object_id
    ;
    
    -- lay sanh sach kpi can chay tong hop
    cursor c_kpi (vv_kpi_register_id number, vv_kpi_group_config_id number, vv_object_type number, vv_object_id number) is
    select vkrr.object_type,
      vkrr.object_id,
      vkrr.kpi_period,
      vkrr.kpi_group_config_id,
      vkrr.kpi_config_id,
      vkrr.kpi_type_code as code,
      vkrr.procedure_code,
      vkrr.plan_type,
      vkrr.max_value
    from KPI_REGISTER_HIS vkrr
    where vkrr.kpi_register_id = vv_kpi_register_id
      and vkrr.kpi_group_config_id = vv_kpi_group_config_id
      and vkrr.object_type = vv_object_type
      and vkrr.object_id = vv_object_id
      and vkrr.kpi_period = 1 -- chu ky
      and vkrr.kpi_period_value = v_cycle_id
    ;
  begin
    v_p_start_time := sysdate;
    v_p_params := '[i_input_date;i_update_kpi_register]=['||
      i_input_date||';'|| i_update_kpi_register||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    -- v_cycle_id  := f_get_cycle_seed(0);
    
    -- lay chu ky hien tai.
    vv_run_date := sysdate;
    select ce.cycle_id, trunc(ce.begin_date), trunc(ce.end_date)
    into v_cycle_id, vv_bccycle_date, vv_eccycle_date
    from CYCLE ce
    where ce.status = 1
      and ce.begin_date < trunc(vv_run_date) + 1
      and ce.end_date >= trunc(vv_run_date);
      
    if (  i_input_date >= vv_bccycle_date
      and i_input_date < vv_eccycle_date + 1
    ) then
      vv_kpi_date := trunc(vv_run_date);
    else
      select ce.cycle_id, trunc(ce.end_date)
      into v_cycle_id, vv_kpi_date
      from CYCLE ce
      where ce.status = 1
        and ce.begin_date < trunc(i_input_date) + 1
        and ce.end_date >= trunc(i_input_date);
    end if;
    
    -- delete from STAFF_TYPE_TMP;
    
    -- t?ng h?p th�ng tin NV + lo?i NV cho ng�y qu� kh?:    
    insert into STAFF_TYPE_TMP (
        staff_id, staff_type_id, prefix
      , specific_type, create_date
    )
    with dta_tmp as (
      select sf.staff_id
        , ste.staff_type_id, ste.prefix, ste.specific_type
        , row_number() over (
            partition by sf.staff_id 
            order by shy.from_date desc, shy.staff_history_id desc
          ) as rn
      from STAFF sf
      join STAFF_HISTORY shy
      on sf.staff_id = shy.staff_id
        and shy.from_date < i_input_date + 1
        and (shy.to_date >= i_input_date or shy.to_date is null)
      join STAFF_TYPE ste 
      on shy.staff_type_id = ste.staff_type_id
        and ste.status = 1
      where sf.status = 1
    )
    select staff_id, staff_type_id, prefix
      , specific_type, sysdate
    from dta_tmp 
    where rn = 1;
    
    -- c?p nh?t ??i t??ng ?k KPI theo d? li?u DB.
    -- SOS: tr??ng h?p ch?y cho qu� kh? c� th? ko c?p nh?t.
    if (i_update_kpi_register = 1) then
      P_KPI_REGISTER_HIS_FCYCLE (vv_kpi_date);
    end if;
    
    for v_kpi_group in c_kpi_group
    loop
      -- tong hop cho tung KPI con trong group
      dbms_output.put_line('run for group: ' || v_kpi_group.group_code);
      for v_kpi in c_kpi(v_kpi_group.kpi_register_id, v_kpi_group.kpi_group_config_id, v_kpi_group.object_type, v_kpi_group.object_id)
      loop
      
        dbms_output.put_line('run procedure: ' || v_kpi.procedure_code);
        if v_kpi.procedure_code = 'P_KPI_AMOUNT_STAFF_CYCLE' then
            -- KPI li�n quan [doanh s?, s?n l??ng; nh�n vi�n; chu k?]
            dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
            P_KPI_AMOUNT_STAFF_CYCLE ( 
                v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
                v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
                v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
                v_kpi.MAX_VALUE, vv_kpi_date );
          /*elsif v_kpi.procedure_code = 'P_KPI_BUY_SHOP_CYCLE' then
            -- KPI li�n quan [doanh s?, s?n l??ng nh?p; npp; chu k?]
            dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
            P_KPI_BUY_SHOP_CYCLE ( 
                v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
                v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
                v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
                v_kpi.MAX_VALUE );*/
        elsif v_kpi.procedure_code = 'P_KPI_CUS_PASS_KS_STAFF_CYCLE' then
          -- KPI li�n quan [S? l??ng ?i?m b�n tr?ng b�y ??t chu?n; nh�n vi�n; chu k?];
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_CUS_PASS_KS_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
          
        elsif v_kpi.procedure_code = 'P_KPI_KSCUSRREG_STAFF_CYCLE' then
          -- KPI li�n quan [kh�ch h�ng ??ng k� ks; nh�n vi�n; chu k?];
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_KSCUSRREG_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
              
        elsif v_kpi.procedure_code = 'P_KPI_PER_CUSKS_STAFF_CYCLE' then
          -- KPI li�n quan [% ?i?m tr?ng b�y ??t chu?n; nh�n vi�n; chu k?];
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_PER_CUSKS_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
              
        elsif v_kpi.procedure_code = 'P_KPI_BUY_STAFF_CYCLE' then
          -- KPI li�n quan [doanh s?, s?n l??ng nh?p; NVQL; chu k?];
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_BUY_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
              
        elsif v_kpi.procedure_code = 'P_KPI_ASO_STAFF_CYCLE' then
          -- KPI do phu (ASO)
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_ASO_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
              
        elsif v_kpi.procedure_code = 'P_KPI_MM_STAFF_CYCLE' then
          -- KPI mo moi
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_MM_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
              
        elsif v_kpi.procedure_code = 'P_KPI_DTDB_STAFF_CYCLE' then
          -- KPI duy tri diem ban
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_DTDB_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
              
        elsif v_kpi.procedure_code = 'P_KPI_AVG_AMOUNT_STAFF_CYCLE' then
          -- KPI chat luong don (trung binh doanh so, san luong)
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_AVG_AMOUNT_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
              
        elsif v_kpi.procedure_code = 'P_KPI_AVG_ORDER_STAFF_CYCLE' then
          -- KPI so luong don thanh cong ( trung binh don thanh cong)
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_AVG_ORDER_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
              
        elsif v_kpi.procedure_code = 'P_KPI_RET_ORDER_STAFF_CYCLE' then
          -- KPI hang tra ve
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_RET_ORDER_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
              
        elsif v_kpi.procedure_code = 'P_KPI_TT_STAFF_CYCLE' then
          -- KPI tang truong
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_TT_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
        elsif v_kpi.procedure_code = 'P_KPI_AVG_KPI_SCORE_SHOP_CYCLE' then
          -- KPI tang truong
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_AVG_KPI_SCORE_SHOP_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
        elsif v_kpi.procedure_code = 'P_KPI_PROMOREG_SFCYCLE' then
          -- KPI tang truong
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_PROMOREG_SFCYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );      
        elsif v_kpi.procedure_code = 'P_KPI_STAFF_OFF_CYCLE' then
          -- KPI tang truong
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_STAFF_OFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );        
        elsif v_kpi.procedure_code = 'P_KPI_AVG_STOCK_SHOP_CYCLE' then
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_AVG_STOCK_SHOP_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
        elsif v_kpi.procedure_code = 'P_KPI_PRO_RETURN_SHOP_CYCLE' then
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_PRO_RETURN_SHOP_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
        elsif v_kpi.procedure_code = 'P_KPI_SUCC_ORDER_SHOP_CYCLE' then
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_SUCC_ORDER_SHOP_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
        else
          dbms_output.put_line('not execute: ' || v_kpi.procedure_code);
        end if;
      end loop;
    end loop;
    
    commit;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
    <<end_procedure>>
    rollback;
    
    -- xoa du lieu tam;
    delete STAFF_TYPE_TMP;
    commit;
  exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1500, 1500));
    raise;
  end P_RUN_KPI_CYCLE;
END PKG_KPI_CYCLE;